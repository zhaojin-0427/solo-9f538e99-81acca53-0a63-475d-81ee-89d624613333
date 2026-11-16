"""FastAPI application: migration control, progress, and metric queries."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import FastAPI, Path, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .config import Settings
from .migration import MigrationEngine, MigrationError
from .reconciliation import (
    ReconciliationError,
    ReconciliationRequest,
    ReconciliationService,
    ReportNotFound,
)
from .repository import (
    DEFAULT_PAGE,
    MAX_PAGE,
    InvalidQuery,
    SampleNotFound,
    SampleRepository,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger("api")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    engine = MigrationEngine(settings)
    repository = SampleRepository(settings)
    reconciliation = ReconciliationService(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine.initialize()
        reconciliation.initialize()
        app.state.settings = settings
        app.state.engine = engine
        app.state.repository = repository
        app.state.reconciliation = reconciliation
        yield
        engine.wait_for_idle(timeout=5)
        reconciliation.wait_for_idle(timeout=5)

    app = FastAPI(
        title="Lab wide-to-long migration API",
        version="1.1.0",
        description=(
            "在线将 samples(id, temperature, ph) 宽表迁移为 sample_metrics 长表；"
            "迁移期间旧采集程序仍可读写，切换后 samples 由兼容视图提供。"
            "另提供夜间清单对账：按提交快照比对，并发写入使报告不可发布，"
            "发布后的写入只把旧报告标记为过期。"
        ),
        lifespan=lifespan,
    )

    def get_engine(request: Request) -> MigrationEngine:
        return request.app.state.engine

    def get_repo(request: Request) -> SampleRepository:
        return request.app.state.repository

    def get_reconciliation(request: Request) -> ReconciliationService:
        return request.app.state.reconciliation

    @app.exception_handler(InvalidQuery)
    async def _invalid_query(_request: Request, exc: InvalidQuery) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(ReconciliationError)
    async def _reconciliation_error(
        _request: Request, exc: ReconciliationError
    ) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(ReportNotFound)
    async def _report_not_found(
        _request: Request, exc: ReportNotFound
    ) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(SampleNotFound)
    async def _not_found(_request: Request, exc: SampleNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(MigrationError)
    async def _migration_error(_request: Request, exc: MigrationError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(RequestValidationError)
    async def _request_validation(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # The default handler echoes the offending input back, which itself
        # fails to serialize when the input was NaN / Infinity.  Report loc +
        # message only.
        errors = [
            {"loc": list(err.get("loc", [])), "msg": err.get("msg", "")}
            for err in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(errors)},
        )

    # ------------------------------------------------------------- migration
    @app.post("/migrations", status_code=202)
    def start_migration(request: Request) -> dict[str, Any]:
        """发起迁移。

        幂等：已完成 → `action=reused`（不重建）；运行中 → 返回当前进度；
        失败 → 从已提交的检查点续跑；从未开始 → 新建并启动后台复制。
        """
        state, started = get_engine(request).start()
        return {
            "action": "started" if started else "reused",
            "migration": state,
            "progress_url": "/migrations/current",
        }

    @app.get("/migrations/current")
    def migration_status(request: Request) -> dict[str, Any]:
        """查询迁移进度与状态（not_started / copying / completed / failed）。"""
        engine = get_engine(request)
        state = engine.get_state()
        # The persisted last_error (if any) is authoritative across restarts;
        # the in-memory one only covers a failure earlier in this process.
        return {
            "migration": state,
            "last_error": state.get("last_error") or engine.last_error,
        }

    @app.get("/migrations/current/events")
    def migration_events(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
    ) -> dict[str, Any]:
        return {"events": get_engine(request).events(limit=limit)}

    # --------------------------------------------------------------- metrics
    @app.get("/metrics/{metric}")
    def list_metric(
        request: Request,
        metric: str,
        after_id: Annotated[int | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = DEFAULT_PAGE,
    ) -> dict[str, Any]:
        """按指标装载长表（键集分页，按 sample_id 递增）。"""
        return get_repo(request).list_metric(metric, after_id=after_id, limit=limit)

    @app.get("/samples/{sample_id}/metrics")
    def sample_metrics(
        request: Request,
        sample_id: Annotated[int, Path()],
    ) -> dict[str, Any]:
        """按样本查询各指标当前值；切换后即长表内容，缺失返回 404。"""
        return get_repo(request).get_metrics(sample_id)

    @app.get("/samples")
    def list_samples(
        request: Request,
        after_id: Annotated[int | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = DEFAULT_PAGE,
    ) -> dict[str, Any]:
        """兼容视图读取（与旧程序看到的 samples 完全一致）。"""
        return get_repo(request).list_samples(after_id=after_id, limit=limit)

    @app.get("/samples/{sample_id}")
    def get_sample(
        request: Request,
        sample_id: Annotated[int, Path()],
    ) -> dict[str, Any]:
        """兼容视图单行读取（旧程序 SELECT 的等价入口）。"""
        return get_repo(request).get_sample(sample_id)

    @app.get("/metrics", include_in_schema=True)
    def metrics_index() -> dict[str, Any]:
        """Available metrics and their query entry points."""
        temperature = {"metric": "temperature", "url": "/metrics/temperature"}
        ph = {"metric": "ph", "url": "/metrics/ph"}
        return {
            "metrics": [temperature, ph],
            "sample_url": "/samples/{sample_id}/metrics",
        }

    # -------------------------------------------------------- reconciliation
    @app.post("/reconciliations", status_code=202)
    def submit_reconciliation(
        request: Request, payload: ReconciliationRequest
    ) -> dict[str, Any]:
        """提交夜间分析清单并发起对账（至多 5000 条互异样本 ID）。

        比对在已提交快照上进行；期间若有任何已提交写入，报告转为
        ``aborted`` 且不留半成品。返回的 ``report_id`` 用于轮询与分页读取。
        """
        run = get_reconciliation(request).submit(payload.entries)
        return {
            "report": run,
            "report_url": f"/reconciliations/{run['report_id']}",
            "diffs_url": f"/reconciliations/{run['report_id']}/diffs",
        }

    @app.get("/reconciliations/{report_id}")
    def read_reconciliation(
        request: Request,
        report_id: Annotated[str, Path(min_length=1, max_length=64)],
    ) -> dict[str, Any]:
        """查询对账报告状态与差异计数；发布后的写入会使其 ``stale=true``。"""
        return {"report": get_reconciliation(request).get_run(report_id)}

    @app.get("/reconciliations/{report_id}/diffs")
    def read_reconciliation_diffs(
        request: Request,
        report_id: Annotated[str, Path(min_length=1, max_length=64)],
        diff_type: Annotated[str | None, Query()] = None,
        after_id: Annotated[int | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = DEFAULT_PAGE,
    ) -> dict[str, Any]:
        """按样本 ID 键集分页读取差异，含两侧原值；显式 NULL 与缺失分开标注。"""
        return get_reconciliation(request).list_diffs(
            report_id, diff_type=diff_type, after_id=after_id, limit=limit
        )

    @app.get("/healthz")
    def healthz(request: Request) -> dict[str, Any]:
        state = get_engine(request).get_state()
        return {
            "status": "ok",
            "migration_state": state.get("state"),
            "view_installed": state.get("view_installed", False),
        }

    return app


app = create_app()
