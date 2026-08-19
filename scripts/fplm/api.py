"""HTTP client for the (reverse-engineered) Fantasy Premier League API.

The API is unauthenticated and generous, but `element-summary` and manager
endpoints mean a full run can be several hundred requests, so the client
retries on transient failures and paces itself.
"""

import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://fantasy.premierleague.com/api"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:42.0) "
    "Gecko/20100101 Firefox/42.0"
)


class FPLNotFound(Exception):
    """Endpoint returned 404 - usually means the resource does not exist yet."""


class FPLClient:
    def __init__(self, delay=0.15, timeout=30, max_retries=4):
        self.delay = delay
        self.timeout = timeout
        self._last_request = 0.0

        retry = Retry(
            total=max_retries,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _pace(self):
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request = time.monotonic()

    def get(self, path):
        """GET a path relative to the API root. Raises on any non-200."""
        self._pace()
        r = self.session.get(f"{BASE_URL}{path}", timeout=self.timeout)
        if r.status_code == 404:
            raise FPLNotFound(path)
        r.raise_for_status()
        return r.json()

    def get_optional(self, path):
        """Like `get`, but returns None for a 404 instead of raising.

        Several endpoints legitimately 404 depending on where we are in the
        season - `entry/{id}/event/{gw}/picks/` does not exist until after
        that gameweek's deadline has passed.
        """
        try:
            return self.get(path)
        except FPLNotFound:
            return None

    # -- endpoint helpers -------------------------------------------------

    def bootstrap(self):
        return self.get("/bootstrap-static/")

    def fixtures(self, gameweek=None):
        suffix = f"?event={gameweek}" if gameweek is not None else ""
        return self.get(f"/fixtures/{suffix}")

    def live(self, gameweek):
        return self.get(f"/event/{gameweek}/live/")

    def element_summary(self, player_id):
        return self.get(f"/element-summary/{player_id}/")

    def entry(self, entry_id):
        return self.get_optional(f"/entry/{entry_id}/")

    def entry_history(self, entry_id):
        return self.get_optional(f"/entry/{entry_id}/history/")

    def entry_transfers(self, entry_id):
        return self.get_optional(f"/entry/{entry_id}/transfers/")

    def entry_picks(self, entry_id, gameweek):
        return self.get_optional(f"/entry/{entry_id}/event/{gameweek}/picks/")

    def league_standings(self, league_id, page=1):
        return self.get_optional(
            f"/leagues-classic/{league_id}/standings/?page_standings={page}"
        )


def derive_season(bootstrap):
    """Return the season label for a bootstrap payload, e.g. '2026-27'.

    The FPL API only ever serves the current season, so the season has to be
    inferred rather than requested. The first gameweek deadline always falls in
    the opening calendar year of the season. Uses the vaastav/FPL convention so
    our own data and the historical backfill share a key.
    """
    deadlines = [e["deadline_time"] for e in bootstrap["events"] if e.get("deadline_time")]
    if not deadlines:
        raise ValueError("bootstrap payload has no event deadlines to derive a season from")
    start_year = int(min(deadlines)[:4])
    return f"{start_year}-{str(start_year + 1)[-2:]}"
