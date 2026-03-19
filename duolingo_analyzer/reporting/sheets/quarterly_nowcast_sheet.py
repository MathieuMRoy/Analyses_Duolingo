"""Renderer de la feuille Nowcast Trimestriel."""

import numbers

from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation


def render_quarterly_nowcast_sheet(
    ws,
    package: dict,
    wb,
    raw_sheet_name: str,
    styles: dict[str, object],
) -> None:
    for merged_range in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(merged_range))
    if ws.max_row:
        ws.delete_rows(1, ws.max_row)

    ws.sheet_view.showGridLines = False
    for column_letter, width in {
        "A": 18,
        "B": 18,
        "C": 18,
        "D": 18,
        "E": 18,
        "F": 18,
        "G": 18,
        "H": 18,
    }.items():
        ws.column_dimensions[column_letter].width = width

    metadata = package.get("metadata", {})
    readiness = package.get("labels_readiness", {})
    assumptions = package.get("assumptions", [])
    historical = package.get("historical_snapshots", [])[-8:]

    raw_ws = wb[raw_sheet_name] if raw_sheet_name in wb.sheetnames else None
    raw_headers = {}
    if raw_ws and raw_ws.max_row >= 1:
        raw_headers = {
            str(cell.value): get_column_letter(idx)
            for idx, cell in enumerate(raw_ws[1], start=1)
            if cell.value
        }

    available_quarters: list[str] = []
    for quarter in metadata.get("available_quarters") or []:
        quarter_label = str(quarter).strip()
        if quarter_label and quarter_label not in available_quarters:
            available_quarters.append(quarter_label)
    if not available_quarters:
        for snapshot in historical:
            quarter_label = str(snapshot.get("quarter") or "").strip()
            if quarter_label and quarter_label not in available_quarters:
                available_quarters.append(quarter_label)

    default_quarter = str(
        metadata.get("default_selected_quarter")
        or metadata.get("current_quarter")
        or (available_quarters[-1] if available_quarters else "N/D")
    )
    if available_quarters and default_quarter not in available_quarters:
        default_quarter = available_quarters[-1]

    quarter_column = raw_headers.get("quarter")
    raw_sheet_ref = f"'{raw_sheet_name}'"

    navy = styles["NAVY"]
    white = styles["WHITE"]
    base_font_name = styles["BASE_FONT_NAME"]
    white_fill = styles["white_fill"]
    center_align = styles["center_align"]
    left_align = styles["left_align"]
    thin_border = styles["thin_border"]

    analyst_surface = "FBFCFE"
    analyst_canvas = "F5F8FB"
    analyst_slate = "EAF0F6"
    analyst_blue_soft = "EEF5FB"
    analyst_green_soft = "EEF7E8"
    analyst_red_soft = "FCEBEC"
    analyst_text = "16324F"
    analyst_muted = "5B6777"
    section_fill = "2A5B84"
    hero_fill = "F3F7FB"
    hero_note_fill = "E8F0F8"
    hero_green = "2E7D5A"
    hero_orange = "B8745F"
    sidebar_fill = "F8FAFD"
    sidebar_label_fill = "DDE7F1"
    muted_zebra_fill = PatternFill(start_color="F7FAFD", end_color="F7FAFD", fill_type="solid")
    model_label_fill = PatternFill(start_color=analyst_slate, end_color=analyst_slate, fill_type="solid")

    def _formula_text_literal(value: str) -> str:
        return '"' + str(value).replace('"', '""') + '"'

    def _raw_lookup_formula(header: str, fallback: str = '"N/D"', quarter_ref: str = "$C$4") -> str:
        if not raw_ws or not quarter_column or header not in raw_headers:
            return f"={fallback}"
        target_column = raw_headers[header]
        return (
            f'=IFERROR(XLOOKUP({quarter_ref},'
            f'{raw_sheet_ref}!${quarter_column}:${quarter_column},'
            f'{raw_sheet_ref}!${target_column}:${target_column},{fallback}),{fallback})'
        )

    def _raw_lookup_expr(header: str, fallback: str = '"N/D"', quarter_ref: str = "$C$4") -> str:
        formula = _raw_lookup_formula(header, fallback=fallback, quarter_ref=quarter_ref)
        return formula[1:] if formula.startswith("=") else formula

    def write_box(
        range_ref: str,
        value: str,
        *,
        fill: str = white,
        font_color: str = "000000",
        size: int = 11,
        bold: bool = False,
        align=None,
        number_format: str | None = None,
    ) -> None:
        if ":" in range_ref:
            ws.merge_cells(range_ref)
            cell = ws[range_ref.split(":")[0]]
        else:
            cell = ws[range_ref]
        cell.value = value
        cell.fill = PatternFill(start_color=fill, end_color=fill, fill_type="solid")
        cell.font = Font(name=base_font_name, size=size, bold=bold, color=font_color)
        cell.alignment = align or center_align
        cell.border = thin_border
        if number_format:
            cell.number_format = number_format

    def write_card(
        start_col: str,
        end_col: str,
        title_row: int,
        title: str,
        value: str,
        note: str,
        accent: str,
        *,
        value_color: str = "000000",
        value_number_format: str | None = None,
        value_fill: str = white,
        note_fill: str = analyst_slate,
    ) -> None:
        write_box(
            f"{start_col}{title_row}:{end_col}{title_row}",
            title,
            fill=accent,
            font_color=white,
            size=10,
            bold=True,
        )
        write_box(
            f"{start_col}{title_row + 1}:{end_col}{title_row + 2}",
            value,
            fill=value_fill,
            font_color=value_color,
            size=16,
            bold=True,
            number_format=value_number_format,
        )
        write_box(
            f"{start_col}{title_row + 3}:{end_col}{title_row + 3}",
            note,
            fill=note_fill,
            font_color=analyst_muted,
            size=9,
            align=Alignment(horizontal="center", vertical="center", wrap_text=True),
        )

    summary_formula = (
        f'="Date snapshot : "&{_raw_lookup_expr("snapshot_as_of_date")}'
        f'&" | Trimestre affiché : "&$C$4'
        f'&" | Statut : "&{_raw_lookup_expr("snapshot_status_label")}'
        f'&" | Jours observés : "&{_raw_lookup_expr("observed_days")}'
        f'&" | Couverture moyenne : "&IFERROR(TEXT({_raw_lookup_expr("avg_coverage_ratio", fallback="0")}, "0.0%"), "N/D")'
        f'&" | Lecture trimestrielle : "&{_raw_lookup_expr("quarter_signal_bias")}'
    )

    write_box("A1:H2", "NOWCAST TRIMESTRIEL", fill=navy, font_color=white, size=18, bold=True)
    write_box(
        "A3:H3",
        summary_formula,
        fill=analyst_slate,
        font_color="333333",
        size=10,
        align=Alignment(horizontal="center", vertical="center", wrap_text=True),
    )

    write_box("A4:B4", "Trimestre affiché", fill=navy, font_color=white, size=10, bold=True)
    write_box("C4", default_quarter, fill=analyst_surface, font_color=analyst_text, size=11, bold=True)
    write_box(
        "D4:H4",
        "Choisissez un trimestre pour consulter son snapshot figé et ses estimations.",
        fill=analyst_canvas,
        font_color=analyst_muted,
        size=9,
        align=Alignment(horizontal="left", vertical="center", wrap_text=True),
    )
    if available_quarters:
        validation = DataValidation(
            type="list",
            formula1=_formula_text_literal(",".join(available_quarters)),
            allow_blank=False,
        )
        validation.promptTitle = "Sélection du trimestre"
        validation.prompt = "Choisissez le trimestre à afficher."
        ws.add_data_validation(validation)
        validation.add(ws["C4"])

    write_box("A5:E5", "Point de lecture trimestriel", fill=navy, font_color=white, size=11, bold=True)
    write_box("F5:H5", "Contexte du trimestre", fill=hero_green, font_color=white, size=11, bold=True)

    write_box(
        "A6:E8",
        '=IFERROR(TEXT(' + _raw_lookup_expr("estimated_revenue_musd", fallback="0") + ', "0.0")&" M$","N/D")',
        fill=hero_fill,
        font_color=analyst_text,
        size=24,
        bold=True,
    )
    write_box(
        "A9:E9",
        _raw_lookup_formula("revenue_note_text"),
        fill=hero_note_fill,
        font_color=analyst_muted,
        size=10,
        align=Alignment(horizontal="center", vertical="center", wrap_text=True),
    )

    write_box("F6:H6", "Lecture trimestrielle", fill=sidebar_label_fill, font_color=analyst_text, size=10, bold=True)
    write_box("F7:H7", _raw_lookup_formula("quarter_signal_bias"), fill=sidebar_fill, font_color=analyst_text, size=16, bold=True)
    write_box("F8:H8", '="Confiance : "&' + _raw_lookup_expr("confidence_level"), fill=sidebar_fill, font_color=analyst_text, size=10)
    write_box(
        "F9:H9",
        '="Taux actifs : "&IFERROR(TEXT(' + _raw_lookup_expr("avg_active_rate", fallback="0") + ', "0.0%"),"N/D")'
        + '&" | Score : "&IFERROR(TEXT(' + _raw_lookup_expr("quarter_signal_score", fallback="0") + ', "0.0")&"/100","N/D")',
        fill=sidebar_fill,
        font_color=analyst_muted,
        size=10,
        align=Alignment(horizontal="center", vertical="center", wrap_text=True),
    )

    write_card(
        "A",
        "B",
        11,
        "Prob. beat revenus",
        _raw_lookup_formula("revenue_beat_probability", fallback="0"),
        "Probabilité implicite sur les revenus du trimestre",
        section_fill,
        value_number_format="0.0%",
        value_fill=analyst_surface,
        note_fill=analyst_blue_soft,
    )
    write_card(
        "C",
        "D",
        11,
        "Prob. guidance raise",
        _raw_lookup_formula("guidance_raise_probability", fallback="0"),
        _raw_lookup_formula("next_guidance_note_text"),
        hero_green,
        value_number_format="0.0%",
        value_fill=analyst_surface,
        note_fill=analyst_green_soft,
    )
    write_card(
        "E",
        "F",
        11,
        "Prob. beat EBITDA",
        _raw_lookup_formula("ebitda_beat_probability", fallback="0"),
        _raw_lookup_formula("ebitda_note_text"),
        hero_orange,
        value_number_format="0.0%",
        value_fill=analyst_surface,
        note_fill="FBF4E4",
    )
    write_card(
        "G",
        "H",
        11,
        "Couverture moyenne",
        _raw_lookup_formula("avg_coverage_ratio", fallback="0"),
        "Moyenne du panel observé sur le trimestre",
        section_fill,
        value_number_format="0.0%",
        value_fill=analyst_surface,
        note_fill=analyst_blue_soft,
    )

    write_box("A16:H16", "Lecture du modèle", fill=navy, font_color=white, size=11, bold=True)
    write_box(
        "A17:H19",
        _raw_lookup_formula("model_summary_text"),
        fill=analyst_surface,
        font_color=analyst_text,
        size=11,
        align=Alignment(horizontal="left", vertical="top", wrap_text=True),
    )

    write_box("A21:D21", "Moteurs principaux", fill=hero_green, font_color=white, size=11, bold=True)
    write_box("E21:H21", "Risques principaux", fill="B85C5C", font_color=white, size=11, bold=True)
    write_box(
        "A22:D25",
        _raw_lookup_formula("main_drivers_text"),
        fill=analyst_green_soft,
        font_color=analyst_text,
        size=10,
        align=Alignment(horizontal="left", vertical="top", wrap_text=True),
    )
    write_box(
        "E22:H25",
        _raw_lookup_formula("main_risks_text"),
        fill=analyst_red_soft,
        font_color=analyst_text,
        size=10,
        align=Alignment(horizontal="left", vertical="top", wrap_text=True),
    )

    next_step = readiness.get("next_step") or "Completer les labels trimestriels avant calibration supervisee."
    model_rows = [
        ("Snapshot selectionne", _raw_lookup_formula("snapshot_status_label")),
        ("Date du snapshot", _raw_lookup_formula("snapshot_as_of_date")),
        (
            "Reference guidance revenus",
            '=IFERROR(IF('
            + _raw_lookup_expr("revenue_guidance_reference_musd", fallback="0")
            + '>0, TEXT('
            + _raw_lookup_expr("revenue_guidance_reference_musd", fallback="0")
            + ', "0.0")&" M$"&IF('
            + _raw_lookup_expr("revenue_guidance_reference_quarter", fallback='""')
            + '<>""," ("&'
            + _raw_lookup_expr("revenue_guidance_reference_quarter", fallback='""')
            + '&")",""), "N/D"), "N/D")',
        ),
        ("Modele supervise pret", "Oui" if readiness.get("supervised_ready") else "Non"),
        ("Etape suivante", next_step),
    ]

    write_box("A27:H27", "Cadre du modele", fill=navy, font_color=white, size=11, bold=True)
    row_cursor = 28
    for label, value in model_rows:
        row_fill = muted_zebra_fill if row_cursor % 2 == 0 else white_fill
        ws[f"A{row_cursor}"] = label
        ws[f"A{row_cursor}"].fill = model_label_fill
        ws[f"A{row_cursor}"].font = Font(name=base_font_name, size=10, bold=True, color=analyst_text)
        ws[f"A{row_cursor}"].alignment = left_align
        ws[f"A{row_cursor}"].border = thin_border
        ws.merge_cells(f"B{row_cursor}:H{row_cursor}")
        value_cell = ws[f"B{row_cursor}"]
        value_cell.value = value
        value_cell.fill = row_fill
        value_cell.font = Font(name=base_font_name, size=10, color=analyst_text)
        value_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        value_cell.border = thin_border
        for merged_col in range(3, 9):
            ws.cell(row=row_cursor, column=merged_col).border = thin_border
            ws.cell(row=row_cursor, column=merged_col).fill = row_fill
        row_cursor += 1

    history_header_row = row_cursor + 1
    write_box(
        f"A{history_header_row}:H{history_header_row}",
        "Historique trimestriel fige",
        fill=navy,
        font_color=white,
        size=11,
        bold=True,
    )
    history_table_row = history_header_row + 1
    history_headers = [
        "Trimestre",
        "Statut",
        "Date snapshot",
        "Score",
        "Beat rev.",
        "Beat EBITDA",
        "Guidance raise",
        "Jours obs.",
    ]
    for col_idx, header in enumerate(history_headers, start=1):
        cell = ws.cell(row=history_table_row, column=col_idx)
        cell.value = header
        cell.fill = PatternFill(start_color=section_fill, end_color=section_fill, fill_type="solid")
        cell.font = Font(name=base_font_name, color=white, bold=True, size=10)
        cell.alignment = center_align
        cell.border = thin_border

    row_cursor = history_table_row + 1
    for idx, snapshot in enumerate(reversed(historical), start=0):
        row_fill = muted_zebra_fill if idx % 2 == 0 else white_fill
        values = [
            snapshot.get("quarter"),
            snapshot.get("snapshot_status_label"),
            snapshot.get("snapshot_as_of_date"),
            snapshot.get("quarter_signal_score"),
            snapshot.get("revenue_beat_probability", snapshot.get("revenue_beat_probability_proxy")),
            snapshot.get("ebitda_beat_probability", snapshot.get("ebitda_beat_probability_proxy")),
            snapshot.get("guidance_raise_probability", snapshot.get("guidance_raise_probability_proxy")),
            snapshot.get("observed_days"),
        ]
        for col_idx, value in enumerate(values, start=1):
            cell = ws.cell(row=row_cursor, column=col_idx)
            cell.value = value
            cell.fill = row_fill
            cell.border = thin_border
            cell.font = Font(name=base_font_name, size=10, color=analyst_text)
            cell.alignment = center_align
            if col_idx == 4 and isinstance(value, numbers.Number):
                cell.number_format = "0.0"
            elif col_idx in {5, 6, 7} and isinstance(value, numbers.Number):
                cell.number_format = "0.0%"
            elif col_idx == 8 and isinstance(value, numbers.Number):
                cell.number_format = "#,##0"
        row_cursor += 1

    assumptions_row = row_cursor + 1
    write_box(f"A{assumptions_row}:H{assumptions_row}", "Hypotheses", fill=navy, font_color=white, size=11, bold=True)
    assumptions_text = "\n".join(f"- {item}" for item in assumptions) if assumptions else "- Aucune hypothese specifique."
    write_box(
        f"A{assumptions_row + 1}:H{assumptions_row + 4}",
        assumptions_text,
        fill=analyst_slate,
        font_color=analyst_muted,
        size=10,
        align=Alignment(horizontal="left", vertical="top", wrap_text=True),
    )

    ws.column_dimensions["A"].width = 18
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 18
    ws.column_dimensions["E"].width = 18
    ws.column_dimensions["F"].width = 15
    ws.column_dimensions["G"].width = 15
    ws.column_dimensions["H"].width = 15

    ws.row_dimensions[3].height = 24
    ws.row_dimensions[4].height = 24
    ws.row_dimensions[5].height = 24
    ws.row_dimensions[6].height = 30
    ws.row_dimensions[7].height = 38
    ws.row_dimensions[8].height = 30
    ws.row_dimensions[9].height = 24
    for row_idx in [12]:
        ws.row_dimensions[row_idx].height = 28
        ws.row_dimensions[row_idx + 1].height = 28
        ws.row_dimensions[row_idx + 2].height = 24
    ws.row_dimensions[16].height = 24
    ws.row_dimensions[17].height = 30
    ws.row_dimensions[18].height = 30
    ws.row_dimensions[19].height = 30
    ws.row_dimensions[21].height = 24
    for row_idx in range(22, 26):
        ws.row_dimensions[row_idx].height = 28
    for row_idx in range(28, history_header_row):
        ws.row_dimensions[row_idx].height = 22
    ws.row_dimensions[history_header_row].height = 24
    ws.row_dimensions[history_table_row].height = 22
    for row_idx in range(history_table_row + 1, row_cursor):
        ws.row_dimensions[row_idx].height = 21
    for row_idx in range(assumptions_row + 1, assumptions_row + 5):
        ws.row_dimensions[row_idx].height = 22
