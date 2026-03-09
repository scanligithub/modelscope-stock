import pandas as pd
import json

class QualityControl:
    def __init__(self):
        self.report = {"files_checked": 0, "errors": [], "summary": {}}

    def check_dataframe(self, df: pd.DataFrame, file_name: str, check_cols: list, file_path: str = ""):
        self.report["files_checked"] += 1
        stats = {"rows": len(df), "columns": len(df.columns), "missing_values": {}}
        
        for col in check_cols:
            if col in df.columns:
                missing = int(df[col].isna().sum())
                if missing > 0:
                    stats["missing_values"][col] = missing
            else:
                self.report["errors"].append(f"Missing column {col} in {file_name}")
                
        self.report["summary"][file_name] = stats

    def save_report(self, path: str):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=4, ensure_ascii=False)

    def get_summary_md(self) -> str:
        md = f"## Data Quality Report\n\n**Total files checked:** {self.report['files_checked']}\n\n### Errors\n"
        if not self.report["errors"]: md += "No errors found.\n"
        else:
            for e in self.report["errors"]: md += f"- {e}\n"
        md += "\n### Stats\n"
        for fname, stats in self.report["summary"].items():
            md += f"- **{fname}**: {stats['rows']} rows, {stats['columns']} cols\n"
        return md
