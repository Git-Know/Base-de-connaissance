import os
import re
import csv

# 📁 Dossiers de projets
project_dirs = ["evershop", "maybe", "metasfresh", "shop-e-commerce"]
output_path = "spark/commits.csv"

# 📝 Création du fichier CSV
with open(output_path, mode="w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["author_name", "module_path", "commit_timestamp"])

    for project in project_dirs:
        filepath = os.path.join(project, "commits.txt")
        if not os.path.exists(filepath):
            continue
        with open(filepath, encoding="utf-8") as f:
            lines = f.readlines()

        for line in lines:
            match = re.search(r"([^-]+)\s*-\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)", line)
            if match:
                author = match.group(1).strip()
                timestamp = match.group(2).strip()
                writer.writerow([author, project, timestamp])
