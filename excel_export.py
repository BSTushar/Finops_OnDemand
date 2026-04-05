from __future__ import annotations
import io
from datetime import datetime
import pandas as pd
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from pricing_engine import CACHE_METADATA


def savings_numeric(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, str):
        if v.strip() == 'No Savings':
            return 0.0
        try:
            return float(v.replace('%', ''))
        except ValueError:
            return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_excel(df: pd.DataFrame, region_label: str) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Recommendations')
        wb = writer.book
        ws = writer.sheets['Recommendations']
        thin = Side(style='thin', color='888888')
        bdr = Border(left=thin, right=thin, top=thin, bottom=thin)
        hdr_fill = PatternFill('solid', fgColor='1A1D23')
        hdr_font = Font(bold=True, color='FFFFFF', size=10)
        for cidx in range(1, ws.max_column + 1):
            c = ws.cell(row=1, column=cidx)
            c.font = hdr_font
            c.fill = hdr_fill
            c.border = bdr
            c.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        sav_cols = [c for c in df.columns if 'Savings %' in c]
        price_cols = [c for c in df.columns if 'Cost ($)' in c]
        green_fill = PatternFill('solid', fgColor='D1FAE5')
        amber_fill = PatternFill('solid', fgColor='FEF3C7')
        red_fill = PatternFill('solid', fgColor='FEE2E2')
        col_list = list(df.columns)
        for ridx in range(2, ws.max_row + 1):
            for cidx in range(1, ws.max_column + 1):
                cell = ws.cell(row=ridx, column=cidx)
                cell.border = bdr
                cell.alignment = Alignment(vertical='center')
                cell.font = Font(size=10)
            for name in sav_cols:
                if name not in col_list:
                    continue
                ci = col_list.index(name) + 1
                sc = ws.cell(row=ridx, column=ci)
                val = sc.value
                nv = savings_numeric(val)
                if nv is None:
                    continue
                if nv >= 20:
                    sc.fill = green_fill
                elif nv > 0:
                    sc.fill = amber_fill
                else:
                    sc.fill = red_fill
            for name in price_cols:
                if name not in col_list:
                    continue
                ci = col_list.index(name) + 1
                ws.cell(row=ridx, column=ci).number_format = '$#,##0.0000'
        for col_cells in ws.columns:
            w = max((len(str(c.value or '')) for c in col_cells), default=8)
            ws.column_dimensions[col_cells[0].column_letter].width = min(w + 2, 48)
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = ws.dimensions
        ws_m = wb.create_sheet('Metadata')
        ws_m.append(['Field', 'Value'])
        ws_m.append(['Generated at', datetime.now().strftime('%Y-%m-%d %H:%M')])
        ws_m.append(['Pricing region', region_label])
        ws_m.append(['Pricing vintage', CACHE_METADATA['last_updated'].strftime('%Y-%m-%d')])
        ws_m.append(['Rows', len(df)])
    return buf.getvalue()
