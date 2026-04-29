from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os

import altair as alt
import pandas as pd
import requests

from dashboard.data_access import LiveDashboardResult, load_live_dashboard_data

try:  # Optional at import time for tests.
    import streamlit as st
except Exception:  # pragma: no cover
    st = None  # type: ignore


AUTO_REFRESH_INTERVAL_SECONDS = 2
DEFAULT_TOPN = 25
DASHBOARD_API_TIMEOUT_SECONDS = 3

ACTIVE_RUN_COLUMNS = [
    "simulation_run_id",
    "status",
    "decision_backend",
    "user_id",
    "started_at",
    "published_actions",
    "published_hand_summaries",
]
RUN_META_COLUMNS = [
    "simulation_run_id",
    "status",
    "decision_backend",
    "user_id",
    "started_at",
    "published_actions",
    "published_hand_summaries",
]
RUN_AGENT_COLUMNS = [
    "player",
    "bb_won",
    "bb_per_100",
    "vpip",
    "pfr",
    "aggression_frequency",
    "observed_hands",
    "observed_actions",
    "actions_per_second",
    "hands_per_second",
]
DASHBOARD_COLUMN_LABELS = {
    "simulation_run_id": "Simulation Run",
    "decision_backend": "Strategy Type",
    "user_id": "Account",
    "started_at": "Started",
    "published_actions": "Actions",
    "published_hand_summaries": "Hands",
    "bb_won": "BB Won",
    "bb_per_100": "BB / 100",
    "vpip": "VPIP",
    "pfr": "PFR",
    "aggression_frequency": "Aggression",
    "observed_hands": "Observed Hands",
    "observed_actions": "Observed Actions",
    "actions_per_second": "Actions / Sec",
    "hands_per_second": "Hands / Sec",
    "showdown_hand_category": "Best Hand",
    "showdown_hand_score": "Hand Score",
    "status": "Status",
    "player": "Player",
}
ACCOUNT_DISPLAY_COLUMNS = [
    "account_name",
    "user_display_name",
    "display_name",
    "user_email",
    "email",
]
PROFIT_LEADERBOARD_COLUMNS = [
    "player",
    "bb_won",
    "bb_per_100",
    "observed_hands",
    "observed_actions",
]
GLOBAL_PROFIT_LEADERBOARD_COLUMNS = [
    "simulation_run_id",
    "player",
    "bb_won",
    "bb_per_100",
    "observed_hands",
    "observed_actions",
]
HIGH_HAND_COLUMNS = [
    "player",
    "showdown_hand_category",
    "showdown_hand_score",
]
GLOBAL_HIGH_HAND_COLUMNS = [
    "simulation_run_id",
    "player",
    "showdown_hand_category",
    "showdown_hand_score",
]


@dataclass(slots=True)
class DashboardSnapshot:
    overview: LiveDashboardResult
    selected_result: LiveDashboardResult


def _require_streamlit() -> None:
    if st is None:  # pragma: no cover
        raise RuntimeError("Streamlit is not installed. Run the dashboard service from Docker Compose.")


def _dashboard_live_api_candidates() -> list[str]:
    configured = [
        os.environ.get("DASHBOARD_LIVE_API_URL"),
        (
            f"{str(os.environ.get('DASHBOARD_APP_URL') or '').rstrip('/')}/api/dashboard/live"
            if os.environ.get("DASHBOARD_APP_URL")
            else None
        ),
        "http://app:3000/api/dashboard/live",
        "http://localhost:3000/api/dashboard/live",
    ]
    candidates: list[str] = []
    seen: set[str] = set()
    for candidate in configured:
        normalized = str(candidate or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(normalized)
    return candidates


def _live_result_from_payload(payload: dict[str, object]) -> LiveDashboardResult:
    return LiveDashboardResult(
        available=bool(payload.get("available")),
        source=str(payload.get("source") or "redis"),
        message=str(payload.get("message") or ""),
        active_run_ids=[
            str(run_id)
            for run_id in (payload.get("active_run_ids") or [])
            if str(run_id or "").strip()
        ],
        active_runs=list(payload.get("active_runs") or []),
        selected_run_id=(
            str(payload.get("selected_run_id"))
            if str(payload.get("selected_run_id") or "").strip()
            else None
        ),
        selected_run_meta=payload.get("selected_run_meta"),
        run_profit_leaderboard=list(payload.get("run_profit_leaderboard") or []),
        run_bb_per_100_leaderboard=list(payload.get("run_bb_per_100_leaderboard") or []),
        run_high_hand_leaderboard=list(payload.get("run_high_hand_leaderboard") or []),
        global_profit_leaderboard=list(payload.get("global_profit_leaderboard") or []),
        global_high_hand_leaderboard=list(payload.get("global_high_hand_leaderboard") or []),
        global_hero_context_leaderboard=list(
            payload.get("global_hero_context_leaderboard") or []
        ),
        run_agents=list(payload.get("run_agents") or []),
        hero_profit_timeseries=list(payload.get("hero_profit_timeseries") or []),
    )


def _load_live_result_for_ui(
    simulation_run_id: str | None,
    user_id: str | None,
    decision_backend: str | None,
    topn: int,
) -> LiveDashboardResult:
    params: dict[str, object] = {"topn": int(topn)}
    if simulation_run_id:
        params["simulation_run_id"] = simulation_run_id
    if user_id:
        params["user_id"] = user_id
    if decision_backend:
        params["decision_backend"] = decision_backend

    for url in _dashboard_live_api_candidates():
        try:
            response = requests.get(
                url,
                params=params,
                timeout=DASHBOARD_API_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and "available" in payload:
                return _live_result_from_payload(payload)
        except Exception:
            continue

    return load_live_dashboard_data(
        simulation_run_id=simulation_run_id,
        user_id=user_id,
        decision_backend=decision_backend,
        topn=topn,
    )


if st is not None:

    @st.cache_data(ttl=1, show_spinner=False)
    def _load_live_result_cached(
        simulation_run_id: str | None,
        user_id: str | None,
        decision_backend: str | None,
        topn: int,
    ) -> LiveDashboardResult:
        return _load_live_result_for_ui(
            simulation_run_id=simulation_run_id,
            user_id=user_id,
            decision_backend=decision_backend,
            topn=topn,
        )

else:  # pragma: no cover

    def _load_live_result_cached(
        simulation_run_id: str | None,
        user_id: str | None,
        decision_backend: str | None,
        topn: int,
    ) -> LiveDashboardResult:
        return _load_live_result_for_ui(
            simulation_run_id=simulation_run_id,
            user_id=user_id,
            decision_backend=decision_backend,
            topn=topn,
        )


def _render_header() -> None:
    st.title("Live Poker Dashboard")
    st.caption(
        "Live run monitoring with automatically refreshing standings and leaderboards."
    )


def _frame_from_rows(rows: list[dict[str, object]] | None, columns: list[str]) -> pd.DataFrame:
    frame = pd.DataFrame(rows or [])
    if frame.empty:
        return frame
    keep_columns = [column for column in columns if column in frame.columns]
    if "user_id" in columns:
        keep_columns.extend(
            column
            for column in ACCOUNT_DISPLAY_COLUMNS
            if column in frame.columns and column not in keep_columns
        )
    if not keep_columns:
        return frame
    return frame[keep_columns].copy()


def _account_display_name(row: pd.Series) -> str:
    for column in ACCOUNT_DISPLAY_COLUMNS:
        value = str(row.get(column) or "").strip()
        if value:
            return value
    return str(row.get("user_id") or "").strip()


def _persona_display_name(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    if normalized == "tag":
        return "TAG"
    if normalized == "lag":
        return "LAG"
    if normalized == "nit":
        return "NIT"
    if normalized == "calling_station":
        return "Calling Station"
    if normalized == "shove_bot":
        return "Shove Bot"
    return normalized.replace("_", " ").title()


def _participant_display_name(row: dict[str, object]) -> str:
    if bool(row.get("is_hero_player")):
        return "Your Agent"
    persona = _persona_display_name(row.get("persona_name"))
    if persona:
        return persona
    player_id = str(row.get("player_id") or "").strip()
    agent_id = str(row.get("agent_id") or "").strip()
    if player_id == "user_agent" or agent_id == "user_agent":
        return "Your Agent"
    return player_id or agent_id or "Unknown"


def _decision_mode_label(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return "n/a"
    if "mixed" in normalized:
        return "Mixed"
    if normalized == "llm" or "llm" in normalized or "model" in normalized:
        return "LLM"
    if normalized == "heuristic" or "heuristic" in normalized or "rules" in normalized:
        return "heuristic"
    return normalized.replace("_", " ").title()


def _display_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    if "user_id" in result.columns:
        result["user_id"] = result.apply(_account_display_name, axis=1)
        result = result.drop(
            columns=[column for column in ACCOUNT_DISPLAY_COLUMNS if column in result.columns]
        )
    if "decision_backend" in result.columns:
        result["decision_backend"] = result["decision_backend"].map(_decision_mode_label)
    if "status" in result.columns:
        result["status"] = result["status"].fillna("n/a").astype(str).str.replace("_", " ").str.title()
    return result.rename(
        columns={
            column: DASHBOARD_COLUMN_LABELS.get(column, column.replace("_", " ").title())
            for column in result.columns
        }
    )


def _display_frame_from_rows(rows: list[dict[str, object]] | None, columns: list[str]) -> pd.DataFrame:
    labeled_rows = []
    for row in rows or []:
        labeled = dict(row)
        labeled["player"] = _participant_display_name(labeled)
        labeled_rows.append(labeled)
    return _frame_from_rows(labeled_rows, columns)


def _normalize_rate_percent(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number < 0:
        return None
    if number <= 1:
        return round(number * 100.0, 2)
    if number <= 100:
        return round(number, 2)
    return None


def _user_agent_row(rows: list[dict[str, object]] | None) -> dict[str, object] | None:
    for row in rows or []:
        if bool(row.get("is_hero_player")):
            return dict(row)
        if _participant_display_name(row) == "Your Agent":
            return dict(row)
    unlabeled_rows = [
        dict(row)
        for row in rows or []
        if not str(row.get("persona_name") or "").strip()
    ]
    if len(unlabeled_rows) == 1:
        return unlabeled_rows[0]
    return None


def _aggression_chart_frame(row: dict[str, object] | None) -> pd.DataFrame:
    if not row:
        return pd.DataFrame()
    points = []
    for label, key in (
        ("VPIP", "vpip"),
        ("PFR", "pfr"),
        ("Aggression", "aggression_frequency"),
    ):
        percent = _normalize_rate_percent(row.get(key))
        if percent is None:
            continue
        points.append({"metric": label, "rate_percent": percent})
    frame = pd.DataFrame(points)
    if frame.empty:
        return frame
    return frame.set_index("metric")


def _render_aggression_chart(frame: pd.DataFrame) -> None:
    chart_frame = frame.reset_index()
    chart = (
        alt.Chart(chart_frame)
        .mark_bar(size=44)
        .encode(
            x=alt.X(
                "metric:N",
                sort=None,
                title=None,
                axis=alt.Axis(labelAngle=0),
            ),
            y=alt.Y(
                "rate_percent:Q",
                title="Percent",
                scale=alt.Scale(domain=[0, 100]),
            ),
            tooltip=[
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("rate_percent:Q", title="Percent", format=".2f"),
            ],
        )
        .properties(height=220)
    )
    st.altair_chart(chart, width="stretch")


def _profit_chart_frame(rows: list[dict[str, object]] | None) -> pd.DataFrame:
    frame = pd.DataFrame(rows or [])
    if frame.empty or "cumulative_bb_won" not in frame.columns:
        return pd.DataFrame()
    if "hand_number" not in frame.columns:
        frame["hand_number"] = range(1, len(frame) + 1)
    frame["hand_number"] = pd.to_numeric(frame["hand_number"], errors="coerce")
    frame["cumulative_bb_won"] = pd.to_numeric(frame["cumulative_bb_won"], errors="coerce")
    frame = frame.dropna(subset=["hand_number", "cumulative_bb_won"]).copy()
    if frame.empty:
        return frame
    frame = frame.sort_values("hand_number", ascending=True)
    return frame.set_index("hand_number")[["cumulative_bb_won"]]


def _profit_chart_hand_count(frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    try:
        return max(0, int(float(frame.index.max())))
    except (TypeError, ValueError):
        return len(frame)


def _normalize_filter_text(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _resolve_selected_run_id(
    active_runs: list[dict[str, object]],
    current_run_id: str | None,
) -> str | None:
    run_ids = [
        str(run.get("simulation_run_id") or "").strip()
        for run in active_runs
        if str(run.get("simulation_run_id") or "").strip()
    ]
    if not run_ids:
        return None
    if current_run_id and current_run_id in run_ids:
        return current_run_id
    return run_ids[0]


def _resolve_selected_snapshot(
    overview: LiveDashboardResult,
    *,
    selected_run_id: str | None,
    user_filter: str | None,
    backend_filter: str | None,
    topn: int,
) -> LiveDashboardResult:
    if selected_run_id is None:
        return overview

    overview_selected_id = str(overview.selected_run_id or "").strip()
    if overview_selected_id and overview_selected_id == str(selected_run_id).strip():
        return overview

    return _load_live_result_cached(
        simulation_run_id=selected_run_id,
        user_id=user_filter,
        decision_backend=backend_filter,
        topn=topn,
    )


def _load_dashboard_snapshot(
    *,
    selected_run_id: str | None,
    user_filter: str | None,
    backend_filter: str | None,
    topn: int,
) -> DashboardSnapshot:
    overview = _load_live_result_cached(
        simulation_run_id=None,
        user_id=user_filter,
        decision_backend=backend_filter,
        topn=topn,
    )
    selected_result = _resolve_selected_snapshot(
        overview,
        selected_run_id=selected_run_id,
        user_filter=user_filter,
        backend_filter=backend_filter,
        topn=topn,
    )
    return DashboardSnapshot(overview=overview, selected_result=selected_result)


def _format_run_option(run: dict[str, object]) -> str:
    run_id = str(run.get("simulation_run_id") or "unknown")
    status = str(run.get("status") or "active")
    strategy = _decision_mode_label(run.get("decision_backend") or "mixed")
    return f"{run_id} | {status} | {strategy}"


def _render_summary_metrics(
    *,
    overview: LiveDashboardResult,
    selected_result: LiveDashboardResult | None,
) -> None:
    selected_run_meta = selected_result.selected_run_meta if selected_result is not None else None
    selected_status = str(selected_run_meta.get("status") or "n/a") if selected_run_meta else "n/a"
    tracked_agents = len(selected_result.run_agents or []) if selected_result is not None else 0
    cards = st.columns(3)
    cards[0].metric("Active Runs", str(len(overview.active_runs or [])))
    cards[1].metric("Selected Status", selected_status)
    cards[2].metric("Tracked Agents", str(tracked_agents))
    refreshed_at = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    st.caption(
        f"Auto-refreshing every {AUTO_REFRESH_INTERVAL_SECONDS} seconds. Last refresh: {refreshed_at}"
    )


def _render_active_runs(active_runs: list[dict[str, object]]) -> None:
    st.subheader("Active Runs")
    frame = _frame_from_rows(active_runs, ACTIVE_RUN_COLUMNS)
    if frame.empty:
        st.info("No live runs are active right now.")
        return
    st.dataframe(_display_frame(frame), width="stretch", hide_index=True)


def _render_run_selector(active_runs: list[dict[str, object]]) -> str | None:
    selected_run_id = _resolve_selected_run_id(
        active_runs,
        st.session_state.get("dashboard_selected_run_id"),
    )
    run_options = {
        str(run.get("simulation_run_id")): _format_run_option(run)
        for run in active_runs
        if str(run.get("simulation_run_id") or "").strip()
    }
    if not run_options:
        st.session_state["dashboard_selected_run_id"] = None
        return None
    st.subheader("Run Focus")
    st.caption("Choose which active run drives the selected-run summaries and live standings below.")
    option_ids = list(run_options.keys())
    selected_index = option_ids.index(selected_run_id) if selected_run_id in option_ids else 0
    chosen_run_id = st.selectbox(
        "Run for Details",
        option_ids,
        index=selected_index,
        format_func=lambda run_id: run_options.get(run_id, run_id),
        key="dashboard_selected_run_id_select",
    )
    st.session_state["dashboard_selected_run_id"] = chosen_run_id
    return chosen_run_id


def _render_selected_run_overview(selected_result: LiveDashboardResult) -> None:
    run_meta = selected_result.selected_run_meta or {}
    st.subheader("Selected Run")
    frame = _frame_from_rows([run_meta], RUN_META_COLUMNS)
    if frame.empty:
        st.info("No live details are available for the selected run yet.")
        return
    st.dataframe(_display_frame(frame), width="stretch", hide_index=True)


def _render_selected_run_live_tables(selected_result: LiveDashboardResult) -> None:
    st.subheader("Selected Run Live Standings")
    profit_frame = _profit_chart_frame(selected_result.hero_profit_timeseries)
    st.markdown("**User Agent Profit Over Run**")
    if profit_frame.empty:
        st.info("The user-agent profit trajectory will appear after hand summaries start landing.")
    else:
        st.line_chart(profit_frame, height=260)
        final_profit = float(profit_frame["cumulative_bb_won"].iloc[-1])
        hand_count = _profit_chart_hand_count(profit_frame)
        st.caption(
            f"Near-real-time cumulative BB won for Your Agent across {hand_count:,} hands. "
            f"Current profit: {final_profit:,.2f} BB."
        )

    user_agent_row = _user_agent_row(selected_result.run_agents)
    aggression_frame = _aggression_chart_frame(user_agent_row)
    st.markdown("**User Agent Aggression Profile**")
    if aggression_frame.empty:
        st.info("The user-agent aggression metrics have not populated yet for this live run.")
    else:
        _render_aggression_chart(aggression_frame)
        observed_hands = user_agent_row.get("observed_hands") if user_agent_row else None
        observed_actions = user_agent_row.get("observed_actions") if user_agent_row else None
        detail_bits = ["Near-real-time VPIP, PFR, and aggression for Your Agent."]
        try:
            if observed_hands is not None:
                detail_bits.append(f"Observed hands: {int(float(observed_hands)):,}")
        except (TypeError, ValueError):
            pass
        try:
            if observed_actions is not None:
                detail_bits.append(f"Observed actions: {int(float(observed_actions)):,}")
        except (TypeError, ValueError):
            pass
        st.caption(" ".join(detail_bits))

    top_left, top_right = st.columns(2)
    with top_left:
        st.markdown("**Profit Leaderboard**")
        st.dataframe(
            _display_frame(
                _display_frame_from_rows(selected_result.run_profit_leaderboard, PROFIT_LEADERBOARD_COLUMNS)
            ),
            width="stretch",
            hide_index=True,
        )
    with top_right:
        st.markdown("**High Hands**")
        st.dataframe(
            _display_frame(
                _display_frame_from_rows(selected_result.run_high_hand_leaderboard, HIGH_HAND_COLUMNS)
            ),
            width="stretch",
            hide_index=True,
        )


def _render_global_leaderboards(overview: LiveDashboardResult) -> None:
    st.subheader("Overall Live Standings")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Profit Leaders**")
        st.dataframe(
            _display_frame(
                _display_frame_from_rows(
                    overview.global_profit_leaderboard,
                    GLOBAL_PROFIT_LEADERBOARD_COLUMNS,
                )
            ),
            width="stretch",
            hide_index=True,
        )
    with col2:
        st.markdown("**High Hands**")
        st.dataframe(
            _display_frame(
                _display_frame_from_rows(
                    overview.global_high_hand_leaderboard,
                    GLOBAL_HIGH_HAND_COLUMNS,
                )
            ),
            width="stretch",
            hide_index=True,
        )


def _render_live_dashboard_fragment_body() -> None:
    filter_columns = st.columns([1.4, 1.0, 0.9])
    with filter_columns[0]:
        user_filter = _normalize_filter_text(
            st.text_input(
                "Account Filter",
                value=st.session_state.get("dashboard_user_filter", ""),
                key="dashboard_user_filter",
                placeholder="Optional account",
            )
        )
    with filter_columns[1]:
        strategy_labels = ["All", "LLM", "heuristic"]
        strategy_values = {"All": None, "LLM": "llm", "heuristic": "heuristic"}
        if st.session_state.get("dashboard_backend_filter") not in strategy_labels:
            st.session_state["dashboard_backend_filter"] = "All"
        strategy_label = st.selectbox(
            "Strategy Filter",
            strategy_labels,
            key="dashboard_backend_filter",
        )
        backend_filter = strategy_values.get(strategy_label)
    with filter_columns[2]:
        topn = int(
            st.selectbox(
                "Rows",
                [10, 25, 50],
                index=1,
                key="dashboard_topn",
            )
        )

    selected_run_id = _normalize_filter_text(st.session_state.get("dashboard_selected_run_id"))
    snapshot = _load_dashboard_snapshot(
        selected_run_id=selected_run_id,
        user_filter=user_filter,
        backend_filter=backend_filter,
        topn=topn,
    )
    overview = snapshot.overview
    if not overview.available:
        st.info(overview.message)
        return

    active_runs = overview.active_runs or []
    selected_run_id = _render_run_selector(active_runs)
    selected_result = (
        _resolve_selected_snapshot(
            overview,
            selected_run_id=selected_run_id,
            user_filter=user_filter,
            backend_filter=backend_filter,
            topn=topn,
        )
        if selected_run_id is not None
        else overview
    )

    _render_summary_metrics(overview=overview, selected_result=selected_result)
    _render_active_runs(active_runs)

    if selected_run_id is not None:
        _render_selected_run_overview(selected_result)
        _render_selected_run_live_tables(selected_result)

    _render_global_leaderboards(overview)


if st is not None:

    @st.fragment(run_every=AUTO_REFRESH_INTERVAL_SECONDS)
    def _render_live_dashboard_fragment() -> None:
        _render_live_dashboard_fragment_body()

else:  # pragma: no cover

    def _render_live_dashboard_fragment() -> None:
        _render_live_dashboard_fragment_body()


def main() -> None:
    _require_streamlit()
    st.set_page_config(page_title="Live Poker Dashboard", page_icon="♠", layout="wide")
    _render_header()
    _render_live_dashboard_fragment()


if __name__ == "__main__":  # pragma: no cover
    main()
