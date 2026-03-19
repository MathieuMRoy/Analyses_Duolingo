"""Renderer de la feuille Signaux Financiers."""

import numbers
import re

from openpyxl.styles import Alignment, Font, PatternFill


def render_financial_nowcast_sheet(
    ws,
    signal_package: dict,
    ia_report: str | None,
    styles: dict[str, object],
    helpers: dict[str, object],
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

    metadata = signal_package.get("metadata", {})
    panel = signal_package.get("panel", {})
    business = signal_package.get("business_signals", {})
    proxy = signal_package.get("financial_proxy_signals", {})
    assumptions = signal_package.get("assumptions", [])
    ai_sections = {
        "RESUME": None,
        "TENDANCES": None,
        "ATTENTION": None,
        "CONSEILS": None,
    }

    if ia_report:
        for key in ai_sections.keys():
            match = re.search(rf"\[{key}\](.*?)(?=\[|$)", ia_report, re.DOTALL)
            if match:
                ai_sections[key] = match.group(1).strip()

    duo_green = styles["DUO_GREEN"]
    duo_blue = styles["DUO_BLUE"]
    navy = styles["NAVY"]
    light_grey = styles["LIGHT_GREY"]
    white = styles["WHITE"]
    base_font_name = styles["BASE_FONT_NAME"]
    zebra_fill = styles["zebra_fill"]
    white_fill = styles["white_fill"]
    center_align = styles["center_align"]
    left_align = styles["left_align"]
    thin_border = styles["thin_border"]

    label_signal_bias = helpers["label_signal_bias"]
    label_confidence = helpers["label_confidence"]
    pretty_fr_number = helpers["pretty_fr_number"]
    pretty_ratio_pct = helpers["pretty_ratio_pct"]
    pretty_score = helpers["pretty_score"]
    pretty_delta_pts = helpers["pretty_delta_pts"]
    compact_summary_text = helpers["compact_summary_text"]
    compact_bullet_text = helpers["compact_bullet_text"]

    def write_box(
        range_ref: str,
        value: str,
        *,
        fill: str = white,
        font_color: str = "000000",
        size: int = 11,
        bold: bool = False,
        align=None,
    ) -> None:
        ws.merge_cells(range_ref)
        cell = ws[range_ref.split(":")[0]]
        cell.value = value
        cell.fill = PatternFill(start_color=fill, end_color=fill, fill_type="solid")
        cell.font = Font(name=base_font_name, size=size, bold=bold, color=font_color)
        cell.alignment = align or center_align
        cell.border = thin_border

    def write_card(
        start_col: str,
        end_col: str,
        title_row: int,
        title: str,
        value: str,
        note: str,
        accent: str,
        value_color: str = "000000",
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
            fill=white,
            font_color=value_color,
            size=17,
            bold=True,
        )
        write_box(
            f"{start_col}{title_row + 3}:{end_col}{title_row + 3}",
            note,
            fill=light_grey,
            font_color="555555",
            size=9,
            align=Alignment(horizontal="center", vertical="center", wrap_text=True),
        )

    bias_label = label_signal_bias(proxy.get("signal_bias"))
    confidence_label = label_confidence(proxy.get("confidence_level"))
    coverage_ratio = panel.get("coverage_ratio")
    observed_users = panel.get("observed_users_today")
    target_users = panel.get("target_panel_size")
    summary_lines = [
        f"Date de reference : {metadata.get('as_of_date', 'N/D')}",
        f"Panel observe : {pretty_fr_number(observed_users, 0)} / {pretty_fr_number(target_users, 0)} "
        f"({pretty_ratio_pct(coverage_ratio, 1)})",
        f"Signal global : {bias_label} | Confiance : {confidence_label}",
    ]

    drivers = proxy.get("main_drivers") or ["Aucun driver majeur identifie pour l'instant."]
    risks = proxy.get("main_risks") or ["Aucun risque majeur identifie pour l'instant."]
    summary_text = (
        f"Signal {bias_label.lower()} avec confiance {confidence_label.lower()}. "
        f"Monetisation {pretty_score(proxy.get('monetization_momentum_index'))}, "
        f"engagement {pretty_score(proxy.get('engagement_quality_index'))}, "
        f"premium 14j {pretty_delta_pts(proxy.get('premium_momentum_14d'))}."
    )
    if ai_sections["RESUME"]:
        summary_text = ai_sections["RESUME"]
    summary_text = compact_summary_text(summary_text, max_sentences=2, max_chars=170)

    bias_fill = {
        "Favorable": duo_green,
        "Neutre": "F4C542",
        "Defavorable": "FF6B6B",
    }.get(bias_label, navy)
    confidence_fill = {
        "Elevee": duo_green,
        "Moyenne": "F4C542",
        "Faible": "FF8A65",
    }.get(confidence_label, navy)

    write_box("A1:H2", "NOWCAST FINANCIER", fill=navy, font_color=white, size=18, bold=True)
    write_box(
        "A3:H3",
        " | ".join(summary_lines),
        fill=light_grey,
        font_color="333333",
        size=10,
        align=Alignment(horizontal="center", vertical="center"),
    )

    write_card("A", "B", 5, "Signal global", bias_label, "Lecture actuelle du signal", bias_fill)
    write_card("C", "D", 5, "Confiance", confidence_label, "Couverture + profondeur historique", confidence_fill)
    write_card(
        "E",
        "F",
        5,
        "Couverture panel",
        pretty_ratio_pct(coverage_ratio, 1),
        "Part du panel observee aujourd'hui",
        duo_blue,
    )
    write_card(
        "G",
        "H",
        5,
        "Panel observe",
        pretty_fr_number(observed_users, 0),
        f"Cible : {pretty_fr_number(target_users, 0)}",
        navy,
    )

    write_card(
        "A",
        "B",
        10,
        "Momentum monetisation",
        pretty_score(proxy.get("monetization_momentum_index")),
        "Indice composite de monetisation",
        duo_green,
    )
    write_card(
        "C",
        "D",
        10,
        "Qualite engagement",
        pretty_score(proxy.get("engagement_quality_index")),
        "Indice composite d'engagement",
        duo_blue,
    )
    write_card(
        "E",
        "F",
        10,
        "Premium momentum 14j",
        pretty_delta_pts(proxy.get("premium_momentum_14d")),
        "Variation recente du taux Super",
        navy,
    )
    churn_value = proxy.get("churn_trend_14d")
    churn_color = duo_green if isinstance(churn_value, numbers.Number) and churn_value <= 0 else "FF6B6B"
    write_card(
        "G",
        "H",
        10,
        "Churn trend 14j",
        pretty_delta_pts(churn_value),
        "Variation recente du taux d'abandon",
        churn_color,
    )

    write_box("A15:H15", "Lecture investisseur", fill=navy, font_color=white, size=11, bold=True)
    write_box(
        "A16:H18",
        summary_text,
        fill=white,
        font_color="000000",
        size=11,
        align=Alignment(horizontal="left", vertical="top", wrap_text=True),
    )

    left_title = "Tendances IA" if ai_sections["TENDANCES"] else "Main Drivers"
    right_title = "Points d'attention" if ai_sections["ATTENTION"] else "Main Risks"
    left_body = compact_bullet_text(
        ai_sections["TENDANCES"] or "\n".join(f"- {item}" for item in drivers),
        max_items=2,
        max_chars=90,
    )
    right_body = compact_bullet_text(
        ai_sections["ATTENTION"] or "\n".join(f"- {item}" for item in risks),
        max_items=2,
        max_chars=90,
    )

    write_box("A20:D20", left_title, fill=duo_green, font_color=white, size=11, bold=True)
    write_box("E20:H20", right_title, fill="FF6B6B", font_color=white, size=11, bold=True)
    write_box(
        "A21:D24",
        left_body,
        fill=white,
        font_color="000000",
        size=10,
        align=Alignment(horizontal="left", vertical="top", wrap_text=True),
    )
    write_box(
        "E21:H24",
        right_body,
        fill=white,
        font_color="000000",
        size=10,
        align=Alignment(horizontal="left", vertical="top", wrap_text=True),
    )

    if ai_sections["CONSEILS"]:
        conclusion_text = compact_summary_text(ai_sections["CONSEILS"], max_sentences=2, max_chars=160)
        write_box("A26:H26", "Conclusion IA", fill=duo_blue, font_color=white, size=11, bold=True)
        write_box(
            "A27:H29",
            conclusion_text,
            fill=light_grey,
            font_color="000000",
            size=10,
            align=Alignment(horizontal="left", vertical="top", wrap_text=True),
        )
        model_header_row = 31
        model_start_row = 32
    else:
        model_header_row = 26
        model_start_row = 27

    write_box(f"A{model_header_row}:H{model_header_row}", "Etat du modele", fill=navy, font_color=white, size=11, bold=True)
    model_rows = [
        ("Reactivation trend 7j", pretty_delta_pts(proxy.get("reactivation_trend_7d"))),
        ("High-value retention", pretty_delta_pts(proxy.get("high_value_retention_trend"))),
        ("Super rate", pretty_ratio_pct(business.get("super_rate"), 1)),
        ("Progression Debutants -> Standard", pretty_ratio_pct(business.get("debutants_to_standard_rate"), 1)),
        ("Abandon Debutants", pretty_ratio_pct(business.get("debutants_abandon_rate"), 1)),
        ("Abandon Standard", pretty_ratio_pct(business.get("standard_abandon_rate"), 1)),
        ("Abandon Super-Actifs", pretty_ratio_pct(business.get("super_actifs_abandon_rate"), 1)),
        (
            "Model readiness",
            "Probabilites implicites disponibles; calibration supervisee a enrichir d'abord avec l'historique de guidance management, puis avec le consensus.",
        ),
    ]
    row_cursor = model_start_row
    for label, value in model_rows:
        row_fill = zebra_fill if row_cursor % 2 == 0 else white_fill
        ws[f"A{row_cursor}"] = label
        ws[f"A{row_cursor}"].fill = row_fill
        ws[f"A{row_cursor}"].font = Font(name=base_font_name, size=10, bold=True)
        ws[f"A{row_cursor}"].alignment = left_align
        ws[f"A{row_cursor}"].border = thin_border
        ws.merge_cells(f"B{row_cursor}:H{row_cursor}")
        value_cell = ws[f"B{row_cursor}"]
        value_cell.value = value
        value_cell.fill = row_fill
        value_cell.font = Font(name=base_font_name, size=10)
        value_cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        value_cell.border = thin_border
        for merged_col in range(3, 9):
            ws.cell(row=row_cursor, column=merged_col).border = thin_border
            ws.cell(row=row_cursor, column=merged_col).fill = row_fill
        row_cursor += 1

    hypotheses_row = row_cursor + 1
    write_box(f"A{hypotheses_row}:H{hypotheses_row}", "Hypotheses & notes", fill=navy, font_color=white, size=11, bold=True)
    assumptions_text = "\n".join(f"- {item}" for item in assumptions) if assumptions else "- Aucune hypothese specifique."
    write_box(
        f"A{hypotheses_row + 1}:H{hypotheses_row + 4}",
        assumptions_text,
        fill=light_grey,
        font_color="333333",
        size=10,
        align=Alignment(horizontal="left", vertical="top", wrap_text=True),
    )

    for row_idx in [6, 11]:
        ws.row_dimensions[row_idx].height = 28
        ws.row_dimensions[row_idx + 1].height = 26
        ws.row_dimensions[row_idx + 2].height = 22
    for row_idx in range(16, 19):
        ws.row_dimensions[row_idx].height = 34
    for row_idx in range(21, 25):
        ws.row_dimensions[row_idx].height = 30
    if ai_sections["CONSEILS"]:
        for row_idx in range(27, 30):
            ws.row_dimensions[row_idx].height = 28
    for row_idx in range(hypotheses_row + 1, hypotheses_row + 5):
        ws.row_dimensions[row_idx].height = 22
