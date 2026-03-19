"""Construction et rendu de la feuille dictionnaire des KPIs."""

from __future__ import annotations

import math

import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill


def build_kpi_dictionary_df() -> pd.DataFrame:
    daily_rows = [
        {
            "Section": "Lecture quotidienne",
            "KPI": "Moyenne Streak (Jours)",
            "Lecture utile": "Longueur moyenne de la serie de jours consecutifs d'utilisation. Mesure la fidelite du panel.",
            "Methode / calcul": "Moyenne simple du streak observe sur le panel du jour.",
        },
        {
            "Section": "Lecture quotidienne",
            "KPI": "Apprentissage (XP/j)",
            "Lecture utile": "Gain moyen de points d'experience depuis la veille. Mesure l'effort d'apprentissage quotidien.",
            "Methode / calcul": "Delta de TotalXP entre hier et aujourd'hui, borne a 0 minimum, puis moyenne sur le panel.",
        },
        {
            "Section": "Lecture quotidienne",
            "KPI": "Taux Abonn. Super",
            "Lecture utile": "Part observable du panel premium via le signal hasPlus. Tant que MAX n'est pas detecte de facon fiable, une partie de MAX peut rester incluse ici.",
            "Methode / calcul": "Profils premium observables / profils observes dans le panel.",
        },
        {
            "Section": "Lecture quotidienne",
            "KPI": "Taux d'Abandon Global",
            "Lecture utile": "Part des utilisateurs actifs hier qui ne le sont plus aujourd'hui.",
            "Methode / calcul": "Utilisateurs avec streak > 0 hier et streak = 0 aujourd'hui / utilisateurs actifs hier.",
        },
        {
            "Section": "Lecture quotidienne",
            "KPI": "Reactivations vs Veille",
            "Lecture utile": "Nombre d'utilisateurs inactifs hier redevenus actifs aujourd'hui.",
            "Methode / calcul": "Comptage des profils avec streak = 0 hier puis streak > 0 aujourd'hui.",
        },
        {
            "Section": "Lecture quotidienne",
            "KPI": "Score d'Engagement",
            "Lecture utile": "Part des profils observes qui restent actifs aujourd'hui.",
            "Methode / calcul": "Profils avec streak > 0 / panel observe du jour.",
        },
    ]

    transition_rows = [
        {
            "Section": "Transitions & churn",
            "KPI": "Progression Debutants vers Standard",
            "Lecture utile": "Part des Debutants actifs hier qui ont progresse vers Standard aujourd'hui sans abandonner.",
            "Methode / calcul": "Debutants actifs hier devenus Standard aujourd'hui / Debutants actifs hier.",
        },
        {
            "Section": "Transitions & churn",
            "KPI": "Abandon Debutants",
            "Lecture utile": "Vrai abandon chez les Debutants, mesure sur la cohorte d'hier et non sur la cohorte du jour.",
            "Methode / calcul": "Debutants actifs hier devenus inactifs aujourd'hui / Debutants actifs hier.",
        },
        {
            "Section": "Transitions & churn",
            "KPI": "Abandon Standard",
            "Lecture utile": "Vrai abandon chez les Standard, mesure sur la cohorte d'hier.",
            "Methode / calcul": "Standard actifs hier devenus inactifs aujourd'hui / Standard actifs hier.",
        },
        {
            "Section": "Transitions & churn",
            "KPI": "Abandon Super-Actifs",
            "Lecture utile": "Vrai abandon chez les Super-Actifs, mesure sur la cohorte d'hier.",
            "Methode / calcul": "Super-Actifs actifs hier devenus inactifs aujourd'hui / Super-Actifs actifs hier.",
        },
    ]

    quarterly_rows = [
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Confiance (trimestriel)",
            "Lecture utile": "Niveau de fiabilite du nowcast trimestriel.",
            "Methode / calcul": "Elevee si couverture moyenne >= 75% et jours observes >= 20 ; Moyenne si couverture >= 45% et jours observes >= 10 ; sinon Faible.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Score trimestre",
            "Lecture utile": "Score synthetique 0-100 du trimestre, utilise pour lire le biais global du nowcast.",
            "Methode / calcul": "Score compose a partir de monetisation, engagement, momentum Super, churn, reactivations et retention high-value, puis ajuste par un breadth factor qui penalise les trimestres encore jeunes.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Prob. beat revenus",
            "Lecture utile": "Probabilite implicite de battre les revenus du trimestre. En facade, c'est la lecture principale du nowcast revenus.",
            "Methode / calcul": "On part d'un score revenus, converti en probabilite implicite. Cette probabilite reste guidance-first : elle se lit face au benchmark management du trimestre tant qu'un consensus complet n'est pas disponible.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Prob. beat EBITDA",
            "Lecture utile": "Probabilite implicite de battre l'EBITDA du trimestre.",
            "Methode / calcul": "Transformation score -> probabilite appliquee a un score EBITDA fonde sur monetisation, engagement, churn, reactivations et retention high-value.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Prob. guidance raise",
            "Lecture utile": "Probabilite implicite d'un relevement de guidance pour le trimestre suivant.",
            "Methode / calcul": "Transformation score -> probabilite appliquee a un score guidance surtout pilote par monetisation, momentum Super, engagement, churn et reactivations.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Estimation revenus trimestrielle",
            "Lecture utile": "Estimation interne des revenus du trimestre, affichee en grand dans le nowcast.",
            "Methode / calcul": "Si une guidance revenus de reference existe, estimation = guidance x (1 + beat implicite). Le beat implicite part du beat historique median et est ajuste par la probabilite revenus, avec bornes de prudence. Sinon, fallback sur le revenu reel du trimestre precedent avec une croissance QoQ implicite bornee.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Est. X M$ vs guidance Y M$",
            "Lecture utile": "Lecture directe de l'estimation revenus par rapport au benchmark management du trimestre.",
            "Methode / calcul": "Le chiffre de gauche est l'estimation du modele. Le chiffre de droite est la guidance revenus du management pour le trimestre suivi, lorsqu'elle est disponible dans l'historique trimestriel.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "EBITDA estime",
            "Lecture utile": "Estimation interne de l'EBITDA ajuste du trimestre.",
            "Methode / calcul": "EBITDA estime = revenus estimes x marge EBITDA implicite. La marge implicite part de la marge historique mediane et est ajustee par la probabilite EBITDA, avec bornes conservatrices.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Guide N+1 estime",
            "Lecture utile": "Estimation interne de la guidance revenus du trimestre suivant.",
            "Methode / calcul": "Guide N+1 estime = revenus estimes x ratio implicite. Le ratio part du ratio historique median guidance suivante / revenus et est ajuste par la probabilite guidance raise.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Historique trimestriel fige",
            "Lecture utile": "Memoire du nowcast tel qu'il etait a la date du snapshot, sans etre ecrase une fois le trimestre termine.",
            "Methode / calcul": "Chaque trimestre garde son snapshot final. Quand un nouveau trimestre commence, le precedent reste fige pour permettre le backtest et la comparaison estimate vs reel.",
        },
        {
            "Section": "Nowcast trimestriel",
            "KPI": "Cadre du modele",
            "Lecture utile": "Bloc qui explique la reference utilisee et le niveau de maturite du modele trimestriel.",
            "Methode / calcul": "Resume du statut du snapshot, de la reference guidance revenus, du niveau de supervision disponible et de la prochaine etape de calibration.",
        },
    ]

    return pd.DataFrame(daily_rows + transition_rows + quarterly_rows)


def render_kpi_dictionary_sheet(ws, styles: dict[str, object]) -> None:
    for merged_range in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(merged_range))
    if ws.max_row:
        ws.delete_rows(1, ws.max_row)

    ws.sheet_view.showGridLines = False
    ws.freeze_panes = "A10"

    for column_letter, width in {
        "A": 15,
        "B": 15,
        "C": 18,
        "D": 18,
        "E": 18,
        "F": 20,
        "G": 20,
        "H": 20,
    }.items():
        ws.column_dimensions[column_letter].width = width

    base_font_name = styles["BASE_FONT_NAME"]
    center_align = styles["center_align"]
    left_align = styles["left_align"]
    thin_border = styles["thin_border"]

    ink = "1E2A36"
    muted = "5D6978"
    paper = "FFFFFF"
    canvas = "F6F4EF"
    stone = "EEE8DD"
    slate = "E9EFF4"
    title = "2E4053"
    bronze = "9E6B2D"
    moss = "4F6F52"
    plum = "6C5B7B"
    warm_blue = "345C7C"
    soft_green = "EEF5ED"
    soft_warm = "FBF4E8"
    soft_plum = "F4EFF7"
    soft_blue = "EEF5FB"

    section_styles = {
        "Lecture quotidienne": {"fill": warm_blue, "soft": soft_blue},
        "Transitions & churn": {"fill": moss, "soft": soft_green},
        "Nowcast trimestriel": {"fill": bronze, "soft": soft_warm},
    }

    def write_box(
        range_ref: str,
        value: str,
        *,
        fill: str = paper,
        font_color: str = ink,
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

    def estimate_lines(*values: str) -> int:
        total = 0
        for value in values:
            text = str(value or "")
            total += max(1, math.ceil(len(text) / 48))
        return total

    rows = build_kpi_dictionary_df().to_dict("records")

    write_box("A1:H2", "GUIDE DES KPIs", fill=title, font_color="FFFFFF", size=18, bold=True)
    write_box(
        "A3:H3",
        "Cette page sert de mode d'emploi. Lisez d'abord la colonne de gauche pour comprendre le sens du KPI, puis utilisez la methode / calcul seulement si vous voulez verifier le detail.",
        fill=canvas,
        font_color=muted,
        size=10,
        align=Alignment(horizontal="center", vertical="center", wrap_text=True),
    )

    write_box("A5:C7", "Commencez par les KPI du Resume Financier Q1 et de Signaux Financiers. Revenez ici seulement quand un indicateur vous pose question.", fill=soft_blue, font_color=ink, size=11, bold=False, align=Alignment(horizontal="left", vertical="top", wrap_text=True))
    write_box("D5:F7", "Ordre de lecture conseille : 1) Lecture quotidienne  2) Transitions & churn  3) Nowcast trimestriel.", fill=soft_green, font_color=ink, size=11, bold=False, align=Alignment(horizontal="left", vertical="top", wrap_text=True))
    write_box("G5:H7", "Astuce : si vous voulez aller vite, ne lisez que la colonne 'Lecture utile'.", fill=soft_plum, font_color=ink, size=11, bold=False, align=Alignment(horizontal="left", vertical="top", wrap_text=True))

    write_box("A9:B9", "KPI", fill=stone, font_color=ink, size=11, bold=True)
    write_box("C9:E9", "Lecture utile", fill=stone, font_color=ink, size=11, bold=True)
    write_box("F9:H9", "Methode / calcul", fill=stone, font_color=ink, size=11, bold=True)

    current_row = 10
    current_section = None

    for row in rows:
        section = row["Section"]
        if section != current_section:
            current_section = section
            section_fill = section_styles.get(section, {"fill": plum})["fill"]
            write_box(
                f"A{current_row}:H{current_row}",
                section,
                fill=section_fill,
                font_color="FFFFFF",
                size=11,
                bold=True,
                align=Alignment(horizontal="left", vertical="center", indent=1),
            )
            ws.row_dimensions[current_row].height = 22
            current_row += 1

        soft_fill = section_styles.get(section, {"soft": slate})["soft"]
        kpi = row["KPI"]
        lecture = row["Lecture utile"]
        methode = row["Methode / calcul"]

        write_box(
            f"A{current_row}:B{current_row}",
            kpi,
            fill=paper,
            font_color=ink,
            size=11,
            bold=True,
            align=Alignment(horizontal="left", vertical="top", wrap_text=True, indent=1),
        )
        write_box(
            f"C{current_row}:E{current_row}",
            lecture,
            fill=soft_fill,
            font_color=ink,
            size=10,
            align=Alignment(horizontal="left", vertical="top", wrap_text=True),
        )
        write_box(
            f"F{current_row}:H{current_row}",
            methode,
            fill=paper,
            font_color=muted,
            size=10,
            align=Alignment(horizontal="left", vertical="top", wrap_text=True),
        )

        ws.row_dimensions[current_row].height = max(30, estimate_lines(kpi, lecture, methode) * 16)
        current_row += 1

    write_box(
        f"A{current_row + 1}:H{current_row + 1}",
        "Le dictionnaire est volontairement pedagogique : il explique d'abord l'usage du KPI, puis sa mecanique. Si un bloc du classeur parait trop dense, c'est ici qu'il faut revenir pour reprendre le fil.",
        fill=canvas,
        font_color=muted,
        size=10,
        align=Alignment(horizontal="left", vertical="center", wrap_text=True),
    )
    ws.row_dimensions[current_row + 1].height = 34

