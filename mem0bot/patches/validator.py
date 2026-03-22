import logging
import re

logger = logging.getLogger(__name__)

# Patterns that indicate a "numeric-only" entity (after normalization)
_NUMERIC_RE = re.compile(r"^\d[\d_.,]*(?:_?(?:usd|eur|btc|eth|sol|m|k|b|%))?$", re.IGNORECASE)

# Patterns that should never be entity names
_BLACKLIST_PATTERNS = [
    re.compile(r"^https?://"),                      # URLs
    re.compile(r"^www\."),                           # URLs without scheme
    re.compile(r"^\d{1,3}\.\d{1,3}\.\d"),           # IP addresses or version numbers
    re.compile(r"^.{80,}$"),                         # Too long (>80 chars)
    re.compile(r"^(the|a|an|is|was|are|were)_"),    # Starts with article/verb
    re.compile(r"_\d{10,}$"),                        # Ends with long number (timestamps)
    re.compile(r"^t\.me/"),                          # Telegram links
    re.compile(r"^@"),                               # Social handles as entities
    re.compile(r"^[^\w]+$"),                         # Only special characters (after underscore normalization)
    re.compile(r"^[\w.+-]+@[\w-]+\.[\w.]+$"),       # Email addresses
    re.compile(r"^\d+(\.\d+)?_?(usd|eur|rub|gbp|jpy|cny|krw|inr|brl|try|aud|cad|chf|hkd|sgd|thb|vnd|php|mxn|pln|czk|sek|nok|dkk|ils|zar|ars|cop|pen|clp|twd|myr|idr|aed|sar|ngn|kzt|uah|gel|amd|uzs|kgs|tjs|azn|byn|mdl|ron|bgn|hrk|rsd|bam|mkd|all|xof|xaf|xdr|btc|eth|sol|bnb|xrp|ada|dot|avax|matic|link|uni|aave|doge|shib|ltc|bch|etc|atom|near|apt|arb|op|ftm|algo|xlm|vet|hbar|icp|fil|egld|sand|mana|axs|gala|flow|theta|kava|celo|one|zil|enj|bat|comp|mkr|snx|crv|sushi|yfi|inch|ldo|rpl|cbeth|steth|wbtc|weth|usdt|usdc|dai|busd|tusd|frax|lusd|gusd|paxg|xaut|m|k|b|t|mm|bn|bps|%)$", re.IGNORECASE),  # Numbers with currency/crypto units
]


class EntityValidator:
    """Structural + domain validation of graph triples.

    Rejects triples that violate quality constraints:
    - Self-loops
    - Pure numeric entities
    - Too short entities (< 2 chars)
    - Too short relationship types (< 3 chars)
    - URLs, email addresses, special-character-only strings
    - Entities that are just numbers with units
    """

    @staticmethod
    def _normalize_entity(name: str) -> str:
        """Normalize entity name: strip whitespace, lowercase."""
        if not name:
            return ""
        return name.strip().lower()

    @staticmethod
    def _is_valid_entity_name(name: str) -> bool:
        """Check if an entity name passes structural quality checks."""
        if not name or not name.strip():
            return False

        normalized = name.strip()

        # Too short (less than 2 characters)
        if len(normalized) < 2:
            return False

        # Check against blacklist patterns
        for pattern in _BLACKLIST_PATTERNS:
            if pattern.match(normalized):
                return False

        # Pure numeric entities (with optional units)
        if _NUMERIC_RE.match(normalized):
            return False

        return True

    @staticmethod
    def _is_valid_relationship(relationship: str) -> bool:
        """Check if a relationship type passes structural quality checks."""
        if not relationship or not relationship.strip():
            return False

        normalized = relationship.strip()

        # Too short (less than 3 characters)
        if len(normalized) < 3:
            return False

        return True

    @classmethod
    def is_valid_triple(cls, source: str, relationship: str, destination: str) -> bool:
        if not source or not relationship or not destination:
            return False

        # Normalize for comparison
        source_norm = cls._normalize_entity(source)
        dest_norm = cls._normalize_entity(destination)
        rel_norm = relationship.strip().lower() if relationship else ""

        # Self-loop
        if source_norm == dest_norm:
            return False

        # Validate entity names
        if not cls._is_valid_entity_name(source_norm):
            logger.debug(f"Rejecting invalid source entity: '{source}' in triple: {source} --[{relationship}]--> {destination}")
            return False

        if not cls._is_valid_entity_name(dest_norm):
            logger.debug(f"Rejecting invalid destination entity: '{destination}' in triple: {source} --[{relationship}]--> {destination}")
            return False

        # Validate relationship type
        if not cls._is_valid_relationship(rel_norm):
            logger.debug(f"Rejecting short relationship: '{relationship}' in triple: {source} --[{relationship}]--> {destination}")
            return False

        return True

    def filter_triples(self, triples: list[dict]) -> list[dict]:
        """Filter a list of {source, relationship, destination} dicts."""
        result = []
        for t in triples:
            if self.is_valid_triple(t.get("source", ""), t.get("relationship", ""), t.get("destination", "")):
                result.append(t)
        return result
