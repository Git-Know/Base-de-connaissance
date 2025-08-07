def match_developer_to_project(developer, project):
    # Supporte "author" ou "author_name"
    dev_name = developer.get("author") or developer.get("author_name") or "unknown"

    match_result = {
        "developer": dev_name,
        "repository": project.get("repository", "unknown"),
        "score": 0,
        "matched_technologies": {
            "languages": [],
            "frameworks": []
        }
    }

    dev_tech = {
        "languages": [tech.lower() for tech in developer.get("languages", [])],
        "frameworks": [tech.lower() for tech in developer.get("frameworks", [])]
    }

    weights = {
        "languages": 3,
        "frameworks": 2
    }

    max_score = sum(
        weights[cat] * len(project.get(cat, []))
        for cat in ["languages", "frameworks"]
    )

    score = 0

    for cat in ["languages", "frameworks"]:
        for tech in project.get(cat, []):
            if tech.lower() in dev_tech[cat]:
                match_result["matched_technologies"][cat].append(tech)
                score += weights[cat]

    percentage_score = (score / max_score) * 100 if max_score > 0 else 0
    contributions = developer.get("contributions") or developer.get("total_contributions", 0)
    bonus = min(contributions / 1000, 1.0) * 10
    final_score = min(percentage_score + bonus, 100)

    match_result["score"] = round(final_score, 2)
    return match_result
