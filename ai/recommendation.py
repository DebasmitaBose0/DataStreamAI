"""
DataStream AI - Personalized OTT Content Recommendation Engine
Uses hybrid content-based filtering, genre affinity weighting, director matching,
and rating prioritization to deliver contextual movie & series recommendations.
"""

from collections import Counter
from typing import List, Dict, Any
import pandas as pd
from database.postgres import get_db


class ContentRecommender:
    def __init__(self):
        self.db = get_db()

    def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """Extracts watch history, preferred genres, and directors for a given user."""
        events_df = self.db.query("SELECT * FROM events;")
        content_df = self.db.query("SELECT * FROM content;")

        if events_df.empty or content_df.empty:
            return {"watched_ids": set(), "favorite_genres": [], "watched_titles": []}

        # Filter events for user that represent content interest
        user_events = events_df[
            (events_df["user_id"] == user_id) &
            (events_df["content_id"].notna()) &
            (events_df["content_id"] != "")
        ]

        if user_events.empty:
            return {"watched_ids": set(), "favorite_genres": [], "watched_titles": []}

        watched_ids = set(user_events["content_id"].unique())
        watched_content = content_df[content_df["content_id"].isin(watched_ids)]

        genres = []
        directors = []
        titles = []
        for _, row in watched_content.iterrows():
            genres.append(row.get("genre", "General"))
            directors.append(row.get("director", ""))
            titles.append(row.get("title", ""))

        genre_counts = Counter(genres)
        director_counts = Counter([d for d in directors if d])

        return {
            "watched_ids": watched_ids,
            "favorite_genres": [g for g, _ in genre_counts.most_common(3)],
            "favorite_directors": [d for d, _ in director_counts.most_common(2)],
            "watched_titles": titles[:4],
            "total_watched_count": len(watched_ids)
        }

    def recommend_for_user(self, user_id: str, top_n: int = 5) -> List[Dict[str, Any]]:
        """Recommends top N unwatched movies/series tailored to the user."""
        content_df = self.db.query("SELECT * FROM content;")
        if content_df.empty:
            return []

        profile = self.get_user_profile(user_id)
        watched_ids = profile["watched_ids"]
        fav_genres = profile["favorite_genres"]
        fav_directors = profile.get("favorite_directors", [])
        watched_titles = profile.get("watched_titles", [])

        # If cold-start user (no watch history), return top rated content
        if not watched_ids:
            top_rated = content_df.sort_values(by="rating", ascending=False).head(top_n)
            recommendations = []
            for _, r in top_rated.iterrows():
                recommendations.append({
                    "content_id": r["content_id"],
                    "title": r["title"],
                    "genre": r["genre"],
                    "content_type": r["content_type"],
                    "rating": r["rating"],
                    "director": r.get("director", "Acclaimed Director"),
                    "match_score": 85,
                    "reason": "🔥 Trending Global Top Pick — Acclaimed by critics"
                })
            return recommendations

        candidates = content_df[~content_df["content_id"].isin(watched_ids)].copy()

        # If user watched everything, include all with different ranking
        if candidates.empty:
            candidates = content_df.copy()

        scored_candidates = []
        for _, item in candidates.iterrows():
            score = 60.0  # Base match baseline

            genre = item.get("genre", "")
            director = item.get("director", "")
            rating = float(item.get("rating", 7.0))

            # Rating contribution (0-20 pts)
            score += (rating - 5.0) * 4.0

            # Genre affinity contribution
            if fav_genres:
                if genre in fav_genres:
                    # Top favorite genre
                    if genre == fav_genres[0]:
                        score += 25.0
                    else:
                        score += 15.0

            # Director affinity contribution
            if director and director in fav_directors:
                score += 15.0

            # Normalized match score capped at 99%
            match_score = int(min(99, max(50, score)))

            # Dynamic AI Explanation Reason
            reasons = []
            if genre in fav_genres:
                reasons.append(f"Matches your affinity for {genre}")
            if director and director in fav_directors:
                reasons.append(f"Directed by {director}")
            if rating >= 8.5:
                reasons.append(f"Masterpiece rating ({rating}/10)")

            if not reasons:
                reasons.append("Trending pick aligned with platform audience")

            primary_reason = f"⭐ {match_score}% Match • " + " • ".join(reasons)

            scored_candidates.append({
                "content_id": item["content_id"],
                "title": item["title"],
                "genre": genre,
                "content_type": item["content_type"],
                "rating": rating,
                "director": director,
                "match_score": match_score,
                "reason": primary_reason
            })

        # Sort by match score descending
        scored_candidates.sort(key=lambda x: (x["match_score"], x["rating"]), reverse=True)
        return scored_candidates[:top_n]

    def get_similar_content(self, content_id: str, top_n: int = 4) -> List[Dict[str, Any]]:
        """Finds items similar to a specific title."""
        content_df = self.db.query("SELECT * FROM content;")
        target = content_df[content_df["content_id"] == content_id]

        if target.empty or content_df.empty:
            return []

        target_row = target.iloc[0]
        target_genre = target_row.get("genre", "")
        target_director = target_row.get("director", "")

        candidates = content_df[content_df["content_id"] != content_id].copy()

        scored = []
        for _, c in candidates.iterrows():
            sim = 50
            if c.get("genre") == target_genre:
                sim += 35
            if c.get("director") == target_director:
                sim += 15
            sim += int((float(c.get("rating", 7.0)) - 7.0) * 4)
            scored.append({
                "content_id": c["content_id"],
                "title": c["title"],
                "genre": c["genre"],
                "rating": c["rating"],
                "similarity_score": min(98, sim)
            })

        scored.sort(key=lambda x: x["similarity_score"], reverse=True)
        return scored[:top_n]


# Global Singleton
content_recommender = ContentRecommender()
