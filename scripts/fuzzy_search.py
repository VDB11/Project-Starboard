from rapidfuzz import fuzz
import re

class FuzzySearch:

    @staticmethod
    def normalize_text(text):
        if not text:
            return ""
        text = text.lower()
        text = re.sub(r'[^\w\s-]', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    @staticmethod
    def tokenize(text):
        return FuzzySearch.normalize_text(text).split()

    @staticmethod
    def calculate_match_score(query, candidate):
        if not query or not candidate:
            return 0

        query_norm = FuzzySearch.normalize_text(query)
        candidate_norm = FuzzySearch.normalize_text(candidate)

        if query_norm in candidate_norm:
            return 100
        if query_norm.lower() in candidate_norm.lower():
            return 100

        token_score   = fuzz.token_sort_ratio(query_norm, candidate_norm)
        partial_score = fuzz.partial_ratio(query_norm, candidate_norm)

        query_tokens     = FuzzySearch.tokenize(query)
        candidate_tokens = FuzzySearch.tokenize(candidate)

        word_match_score = 0
        if query_tokens and candidate_tokens:
            for q_word in query_tokens:
                for c_word in candidate_tokens:
                    if q_word in c_word or c_word in q_word:
                        word_match_score = max(word_match_score, 95)
                    else:
                        word_sim = fuzz.ratio(q_word, c_word)
                        if word_sim >= 60:
                            word_match_score = max(word_match_score, word_sim)

        overall_score = fuzz.ratio(query_norm, candidate_norm)

        misspelling_score = 0
        variations = [
            query_norm.replace('a', 'e'),
            query_norm.replace('e', 'a'),
            query_norm.replace('i', 'e'),
            query_norm.replace('e', 'i'),
            query_norm.replace('o', 'a'),
            query_norm.replace('a', 'o'),
            query_norm.replace('u', ''),
            query_norm.replace('th', 't'),
        ]
        for variation in variations:
            if variation in candidate_norm:
                misspelling_score = 90
                break

        combined_score = max(
            token_score * 0.25 + partial_score * 0.35 + word_match_score * 0.4,
            overall_score,
            misspelling_score
        )

        return combined_score

    @staticmethod
    def search(query, candidates, limit=50, threshold=85):
        if not candidates:
            return []
        if not query:
            return [{'id': c, 'name': c, 'score': 100} for c in candidates[:limit]]

        results = []
        for candidate in candidates:
            score = FuzzySearch.calculate_match_score(query, candidate)
            if score >= threshold:
                results.append({'id': candidate, 'name': candidate, 'score': score})

        results.sort(key=lambda x: x['score'], reverse=True)
        return results[:limit]