
import sys
from pathlib import Path
import duolingo_analyzer.stats as stats
import duolingo_analyzer.agent as agent

if __name__ == "__main__":
    print("🚀 Regeneration of existing report for verification...")
    
    # Simulate completion of stats calculation
    result_stats = stats.calculer_statistiques()
    
    if result_stats:
        # Generate IA report (or use placeholder if API fails)
        ia_report = agent.generer_rapport_ia(result_stats)
        
        # This will trigger the updated Excel generation logic
        stats.sauvegarder_rapport_excel(result_stats, ia_report)
        print("\n✅ Verification report generated!")
    else:
        print("❌ Could not calculate stats.")
