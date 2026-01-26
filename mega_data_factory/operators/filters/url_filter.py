"""
URL Filter

Filters records based on URL criteria using the approach from RefinedWeb (arXiv:2306.01116).
Implements three-part URL filtering from Section G.1:
1. Domain blocklist - filters adult/spam/fraudulent domains
2. URL word scoring - scores URLs based on word severity weights
3. High-quality source exclusion - excludes curated sources to prevent overlap

Reference: https://arxiv.org/pdf/2306.01116 Section G.1
"""

import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from mega_data_factory.framework import Filter

FIELD_URL = "url"

# Default adult/inappropriate word list with severity weights (0-1)
# Higher weight = more severe, more likely to filter
DEFAULT_WORD_WEIGHTS: dict[str, float] = {
    # Adult content (high severity)
    "porn": 1.0,
    "xxx": 1.0,
    "sex": 0.8,
    "adult": 0.6,
    "nude": 0.9,
    "naked": 0.9,
    "erotic": 0.9,
    "nsfw": 1.0,
    "hentai": 1.0,
    "webcam": 0.5,
    "escort": 0.8,
    "fetish": 0.9,
    # Gambling (high severity)
    "casino": 0.9,
    "poker": 0.7,
    "gambling": 0.9,
    "betting": 0.8,
    "slot": 0.6,
    "jackpot": 0.7,
    # Violence/illegal (high severity)
    "gore": 0.9,
    "hack": 0.5,
    "crack": 0.5,
    "warez": 0.9,
    "torrent": 0.6,
    "pirate": 0.6,
    # Spam/fraud indicators (medium severity)
    "free-money": 0.8,
    "get-rich": 0.8,
    "click-here": 0.5,
    "buy-now": 0.4,
    "limited-offer": 0.5,
}

# Default high-quality source domains to exclude (Section G.1.3)
# These are filtered to prevent overlap with curated corpora
DEFAULT_EXCLUDED_QUALITY_SOURCES: set[str] = {
    # Knowledge bases
    "wikipedia.org",
    "wikimedia.org",
    "wikidata.org",
    "wikisource.org",
    "wikibooks.org",
    "wikiquote.org",
    "wikinews.org",
    "wikivoyage.org",
    "wiktionary.org",
    # Academic/research
    "arxiv.org",
    "pubmed.gov",
    "ncbi.nlm.nih.gov",
    "scholar.google.com",
    "semanticscholar.org",
    "acm.org",
    "ieee.org",
    "springer.com",
    "sciencedirect.com",
    "nature.com",
    "plos.org",
    # Code repositories
    "github.com",
    "gitlab.com",
    "bitbucket.org",
    "sourceforge.net",
    # Q&A / Documentation
    "stackoverflow.com",
    "stackexchange.com",
    "superuser.com",
    "serverfault.com",
    "askubuntu.com",
    "mathoverflow.net",
    # Books / Literature
    "gutenberg.org",
    "archive.org",
    "openlibrary.org",
    # Reference
    "britannica.com",
    "encyclopedia.com",
}


class URLFilter(Filter):
    """Filter records based on URL criteria.

    Implements the three-part URL filtering approach from RefinedWeb (arXiv:2306.01116 Section G.1):
    1. Domain blocklist - filters adult/spam/fraudulent domains (G.1.1)
    2. URL word scoring - scores URLs based on word severity weights (G.1.2)
    3. High-quality source exclusion - excludes curated sources to prevent overlap (G.1.3)

    URLs are filtered if:
    - Domain is in the blocklist, OR
    - URL score exceeds the threshold, OR
    - Domain is a high-quality source (when exclusion is enabled)
    """

    def __init__(
        self,
        url_field: str = FIELD_URL,
        blocklist_paths: list[str] | None = None,
        blocklist_domains: list[str] | None = None,
        word_weights: dict[str, float] | None = None,
        use_default_words: bool = True,
        score_threshold: float = 0.5,
        check_subdomains: bool = True,
        exclude_quality_sources: bool = False,
        quality_source_domains: list[str] | None = None,
        use_default_quality_sources: bool = True,
    ):
        """Initialize URL filter.

        Args:
            url_field: Name of the URL field in records.
            blocklist_paths: Paths to files containing blocked domains (one per line).
            blocklist_domains: List of domains to block directly.
            word_weights: Dict mapping words to severity weights (0-1).
            use_default_words: Whether to include default adult/spam word list.
            score_threshold: URL score threshold for filtering (0-1).
                            URLs with score >= threshold are filtered out.
            check_subdomains: Whether to check if URL domain is subdomain of blocked domain.
            exclude_quality_sources: Whether to exclude high-quality source domains (G.1.3).
                                    Enable this to prevent overlap with curated corpora.
            quality_source_domains: Additional domains to treat as high-quality sources.
            use_default_quality_sources: Whether to use default quality source list
                                        (Wikipedia, arXiv, GitHub, etc.).
        """
        super().__init__()
        self.url_field = url_field
        self.score_threshold = score_threshold
        self.check_subdomains = check_subdomains
        self.exclude_quality_sources = exclude_quality_sources

        # Build domain blocklist
        self._blocked_domains: set[str] = set()
        if blocklist_domains:
            for domain in blocklist_domains:
                self._blocked_domains.add(domain.lower().strip())

        if blocklist_paths:
            for path in blocklist_paths:
                self._load_blocklist(path)

        # Build high-quality source exclusion list (G.1.3)
        self._quality_source_domains: set[str] = set()
        if exclude_quality_sources:
            if use_default_quality_sources:
                self._quality_source_domains.update(DEFAULT_EXCLUDED_QUALITY_SOURCES)
            if quality_source_domains:
                for domain in quality_source_domains:
                    self._quality_source_domains.add(domain.lower().strip())

        # Build word weights
        self._word_weights: dict[str, float] = {}
        if use_default_words:
            self._word_weights.update(DEFAULT_WORD_WEIGHTS)
        if word_weights:
            self._word_weights.update(word_weights)

        # Pre-compile word pattern for efficiency
        if self._word_weights:
            # Sort by length descending to match longer patterns first
            words = sorted(self._word_weights.keys(), key=len, reverse=True)
            escaped = [re.escape(w) for w in words]
            self._word_pattern = re.compile(r"(" + "|".join(escaped) + r")", re.IGNORECASE)
        else:
            self._word_pattern = None

    def _load_blocklist(self, path: str) -> None:
        """Load blocked domains from a file (one domain per line)."""
        filepath = Path(path)
        if not filepath.exists():
            print(f"Warning: Blocklist file not found: {path}")
            return

        with open(filepath, encoding="utf-8") as f:
            for line in f:
                domain = line.strip().lower()
                if domain and not domain.startswith("#"):
                    self._blocked_domains.add(domain)

        print(f"Loaded {len(self._blocked_domains)} domains from blocklist")

    def _extract_domain(self, url: str) -> str | None:
        """Extract domain from URL."""
        try:
            if not url.startswith(("http://", "https://")):
                url = "http://" + url
            parsed = urlparse(url)
            return parsed.netloc.lower() if parsed.netloc else None
        except Exception:
            return None

    def _is_domain_blocked(self, domain: str) -> bool:
        """Check if domain is in blocklist."""
        if not domain:
            return False

        # Direct match
        if domain in self._blocked_domains:
            return True

        # Check if domain is subdomain of blocked domain
        if self.check_subdomains:
            parts = domain.split(".")
            for i in range(len(parts) - 1):
                parent = ".".join(parts[i + 1 :])
                if parent in self._blocked_domains:
                    return True

        return False

    def _is_quality_source(self, domain: str) -> bool:
        """Check if domain is a high-quality source to exclude (G.1.3)."""
        if not domain or not self._quality_source_domains:
            return False

        # Direct match
        if domain in self._quality_source_domains:
            return True

        # Check if domain is subdomain of quality source
        if self.check_subdomains:
            parts = domain.split(".")
            for i in range(len(parts) - 1):
                parent = ".".join(parts[i + 1 :])
                if parent in self._quality_source_domains:
                    return True

        return False

    def _compute_url_score(self, url: str) -> float:
        """Compute URL score based on word weights.

        Returns a score between 0 and 1, where higher scores indicate
        more likely inappropriate content.
        """
        if not self._word_pattern or not url:
            return 0.0

        url_lower = url.lower()
        matches = self._word_pattern.findall(url_lower)

        if not matches:
            return 0.0

        # Aggregate scores - use max score among matched words
        # This follows the "severity" approach from RefinedWeb
        max_score = 0.0
        total_score = 0.0
        for match in matches:
            weight = self._word_weights.get(match.lower(), 0.0)
            max_score = max(max_score, weight)
            total_score += weight

        # Combine max and cumulative (weighted toward max)
        # Multiple matches increase score slightly
        combined = max_score * 0.8 + min(total_score / 3.0, 0.2)
        return min(combined, 1.0)

    def should_keep_batch(self, records: list[dict[str, Any]]) -> list[bool]:
        """Determine which records pass URL filtering.

        Applies three-part filtering from RefinedWeb Section G.1:
        1. Domain blocklist check (G.1.1)
        2. URL word scoring (G.1.2)
        3. High-quality source exclusion (G.1.3)
        """
        results = []
        for record in records:
            url = record.get(self.url_field)

            # Keep records without URL field
            if not url or not isinstance(url, str):
                results.append(True)
                continue

            domain = self._extract_domain(url)

            # G.1.1: Check domain blocklist
            if self._is_domain_blocked(domain):
                results.append(False)
                continue

            # G.1.2: Check URL word score
            score = self._compute_url_score(url)
            if score >= self.score_threshold:
                results.append(False)
                continue

            # G.1.3: Check high-quality source exclusion
            if self.exclude_quality_sources and self._is_quality_source(domain):
                results.append(False)
                continue

            results.append(True)

        return results
