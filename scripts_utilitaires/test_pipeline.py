import sys
from pathlib import Path

# Override config targets BEFORE importing modules
import duolingo_analyzer.config as cfg
cfg.OBJECTIF_PAR_COHORTE = 10
cfg.OBJECTIF_UTILISATEURS = 30

import main

if __name__ == "__main__":
    # Remove existing files to force a fresh test run
    csv1 = Path("target_users.csv")
    csv2 = Path("daily_streaks_log.csv")
    
    if csv1.exists():
        csv1.unlink()
    if csv2.exists():
        csv2.unlink()
        
    print(f"--- TEST RUN: {cfg.OBJECTIF_PAR_COHORTE} users per cohort ---")
    main.executer_pipeline_global() if hasattr(main, 'executer_pipeline_global') else main
