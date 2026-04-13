"""
Rich live dashboard — real-time P&L, positions, signals, system health.
Run in background; refresh every 2 seconds.
"""
from __future__ import annotations
import asyncio
import time
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.text import Text
from core.models import PortfolioState
from core.calibration import get_strategy_calibration_report
import config

console = Console()


def _build_portfolio_panel(portfolio: PortfolioState, gateway_stats: dict) -> Panel:
    total = portfolio.total_value
    pnl = portfolio.unrealized_pnl + portfolio.realized_pnl
    pnl_color = "green" if pnl >= 0 else "red"
    drawdown_color = "red" if portfolio.drawdown > 0.10 else "yellow" if portfolio.drawdown > 0.05 else "green"

    lines = [
        f"[bold]Bankroll:[/bold]        ${portfolio.bankroll:>10,.2f}",
        f"[bold]Total Value:[/bold]     ${total:>10,.2f}",
        f"[bold]P&L:[/bold]             [{pnl_color}]${pnl:>+10,.2f}[/{pnl_color}]",
        f"[bold]Drawdown:[/bold]        [{drawdown_color}]{portfolio.drawdown:.1%}[/{drawdown_color}]",
        f"[bold]Trade Count:[/bold]     {portfolio.trade_count:>10,}",
        f"[bold]Mode:[/bold]            {'[yellow]DRY RUN[/yellow]' if config.DRY_RUN else '[green]LIVE[/green]'}",
        f"[bold]Submitted:[/bold]       {gateway_stats.get('submitted', 0):>10,}",
        f"[bold]Rejected:[/bold]        {gateway_stats.get('rejected', 0):>10,}",
    ]
    return Panel("\n".join(lines), title="[bold cyan]Portfolio[/bold cyan]", border_style="cyan")


def _build_positions_table(portfolio: PortfolioState) -> Table:
    table = Table(title="Open Positions", show_header=True, header_style="bold magenta")
    table.add_column("Market", max_width=30)
    table.add_column("Side")
    table.add_column("Entry")
    table.add_column("Current")
    table.add_column("Size")
    table.add_column("P&L")
    table.add_column("Strategy")

    for pos in list(portfolio.positions.values())[:15]:
        pnl = pos.unrealized_pnl
        pnl_str = f"[green]${pnl:+,.2f}[/green]" if pnl >= 0 else f"[red]${pnl:+,.2f}[/red]"
        table.add_row(
            pos.condition_id[:28],
            pos.side,
            f"{pos.avg_entry_price:.4f}",
            f"{pos.current_price:.4f}",
            f"${pos.notional_usd:.0f}",
            pnl_str,
            pos.strategy[:12],
        )

    if not portfolio.positions:
        table.add_row("[dim]No open positions[/dim]", "", "", "", "", "", "")

    return table


def _build_calibration_table() -> Table:
    table = Table(title="Strategy Calibration", show_header=True, header_style="bold blue")
    table.add_column("Strategy")
    table.add_column("Trades")
    table.add_column("Accuracy")
    table.add_column("Cal.Error")
    table.add_column("Kelly%")

    report = get_strategy_calibration_report()
    for strategy, stats in sorted(report.items(), key=lambda x: x[1]["trades"], reverse=True):
        acc = stats["accuracy"]
        err = stats["calibration_error"]
        kelly = stats["kelly_fraction"]
        acc_color = "green" if acc > 0.55 else "yellow" if acc > 0.45 else "red"
        table.add_row(
            strategy,
            str(stats["trades"]),
            f"[{acc_color}]{acc:.1%}[/{acc_color}]",
            f"{err:.3f}",
            f"{kelly:.1%}",
        )

    if not report:
        table.add_row("[dim]No calibration data yet[/dim]", "", "", "", "")

    return table


async def run_dashboard(portfolio: PortfolioState, gateway_stats: dict):
    """Run the live dashboard in the terminal."""
    layout = Layout()
    layout.split_column(
        Layout(name="top", size=12),
        Layout(name="middle"),
        Layout(name="bottom"),
    )
    layout["top"].split_row(
        Layout(name="portfolio"),
        Layout(name="spacer"),
    )

    with Live(layout, console=console, refresh_per_second=0.5, screen=False) as live:
        while True:
            layout["portfolio"].update(
                _build_portfolio_panel(portfolio, gateway_stats)
            )
            layout["middle"].update(
                Panel(_build_positions_table(portfolio), border_style="magenta")
            )
            layout["bottom"].update(
                Panel(_build_calibration_table(), border_style="blue")
            )
            await asyncio.sleep(2)
