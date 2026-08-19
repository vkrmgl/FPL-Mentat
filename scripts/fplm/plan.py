"""Multi-gameweek transfer and chip planning.

Extends the single-gameweek squad problem across a horizon, which is where the
interesting constraints live: free transfers accumulate, extra transfers cost
four points, and chips can only be played once per half of the season.

Horizon
-------
Five gameweeks by default, chosen from the rules rather than by taste. FPL's
`max_extra_free_transfers` is 4, so the bank caps at five - there is no
save-and-spend strategy expressible over six weeks that is not already
expressible over five, and plans are re-solved weekly anyway, so precision at
week five matters little. Chip timing is a separate, coarser pass over a longer
window, because it is a one-shot decision per half rather than a disposable
weekly plan.

Free transfer accounting
------------------------
The natural statement, `banked_next = min(5, banked - used + 1)`, is nonlinear.
It is expressed here as a pair of inequalities plus a maximising objective,
which is exact for this problem because banking transfers is never penalised -
the solver has no incentive to under-report what it saved.

Simplification, stated rather than hidden: prices are held fixed across the
horizon. Real price drift is around 0.1 per week for an active player, which is
small next to a 4-point hit, and modelling it properly needs a price-change
model that does not exist yet.
"""

import numpy as np
import pandas as pd
from scipy.optimize import Bounds, LinearConstraint, milp

TRANSFER_HIT = 4.0
MAX_BANKED_TRANSFERS = 5


class HorizonPlanner:
    def __init__(self, squad_rules, budget=100.0, club_limit=3, squad_size=15,
                 starting_size=11, transfer_hit=TRANSFER_HIT,
                 max_banked=MAX_BANKED_TRANSFERS):
        if squad_rules is None or squad_rules.empty:
            raise ValueError("squad_rules is empty - position constraints would be dropped")
        self.squad_rules = squad_rules.set_index("position")
        self.budget = budget
        self.club_limit = club_limit
        self.squad_size = squad_size
        self.starting_size = starting_size
        self.transfer_hit = transfer_hit
        self.max_banked = max_banked

    def solve(self, players, projections, gameweeks, current_squad=None,
              free_transfers=1, bench_weight=0.1):
        """Plan transfers and line-ups across `gameweeks`.

        `projections` is player_id x gameweek expected points. `current_squad`
        is the squad held going in; passing None plans from scratch, which is
        what a wildcard amounts to.

        Bench players contribute at `bench_weight` - they only score if someone
        ahead of them does not play, so they are worth something but not full
        value. Setting it to 1.0 turns this into a bench-boost plan.
        """
        players = players.reset_index(drop=True)
        n, w = len(players), len(gameweeks)
        pid_index = {p: i for i, p in enumerate(players.player_id)}

        value = np.zeros((n, w))
        for j, gw in enumerate(gameweeks):
            col = projections.get(gw, {})
            for pid, xp in col.items():
                if pid in pid_index:
                    value[pid_index[pid], j] = xp

        price = players["price"].to_numpy(dtype=float)
        position = players["position"].to_numpy()
        team = players["team_id"].to_numpy()

        # Variable layout: squad[n,w] | start[n,w] | captain[n,w] | buy[n,w]
        #                | sell[n,w]  | banked[w]  | hits[w]
        blocks = {"squad": 0, "start": n * w, "captain": 2 * n * w,
                  "buy": 3 * n * w, "sell": 4 * n * w}
        banked_at = 5 * n * w
        hits_at = banked_at + w
        total_vars = hits_at + w

        def idx(block, i, j):
            return blocks[block] + i * w + j

        # Objective: starters at full value, bench discounted, captain doubled,
        # minus four points per hit. Banked transfers carry a small positive
        # weight so the solver prefers keeping them when otherwise indifferent.
        c = np.zeros(total_vars)
        for i in range(n):
            for j in range(w):
                c[idx("start", i, j)] = -value[i, j] * (1 - bench_weight)
                c[idx("squad", i, j)] = -value[i, j] * bench_weight
                c[idx("captain", i, j)] = -value[i, j]
        for j in range(w):
            c[hits_at + j] = self.transfer_hit
            c[banked_at + j] = -0.01

        cons = []

        def row():
            return np.zeros(total_vars)

        for j in range(w):
            # Squad size, XI size, one captain.
            r = row()
            for i in range(n):
                r[idx("squad", i, j)] = 1
            cons.append(LinearConstraint(r, self.squad_size, self.squad_size))

            r = row()
            for i in range(n):
                r[idx("start", i, j)] = 1
            cons.append(LinearConstraint(r, self.starting_size, self.starting_size))

            r = row()
            for i in range(n):
                r[idx("captain", i, j)] = 1
            cons.append(LinearConstraint(r, 1, 1))

            # Budget.
            r = row()
            for i in range(n):
                r[idx("squad", i, j)] = price[i]
            cons.append(LinearConstraint(r, 0, self.budget))

            # Position composition and legal formation.
            for pos, rule in self.squad_rules.iterrows():
                mask = position == pos
                r = row()
                for i in np.flatnonzero(mask):
                    r[idx("squad", i, j)] = 1
                cons.append(LinearConstraint(r, rule.squad_count, rule.squad_count))

                r = row()
                for i in np.flatnonzero(mask):
                    r[idx("start", i, j)] = 1
                cons.append(LinearConstraint(r, rule.min_playing, rule.max_playing))

            # Club limit.
            for club in np.unique(team):
                r = row()
                for i in np.flatnonzero(team == club):
                    r[idx("squad", i, j)] = 1
                cons.append(LinearConstraint(r, 0, self.club_limit))

            # Starter implies squad member; captain implies starter.
            for i in range(n):
                r = row()
                r[idx("squad", i, j)] = -1
                r[idx("start", i, j)] = 1
                cons.append(LinearConstraint(r, -np.inf, 0))

                r = row()
                r[idx("start", i, j)] = -1
                r[idx("captain", i, j)] = 1
                cons.append(LinearConstraint(r, -np.inf, 0))

            # Squad continuity: this week's squad is last week's, plus buys,
            # minus sells. The first week is anchored to the squad held now.
            for i in range(n):
                r = row()
                r[idx("squad", i, j)] = 1
                r[idx("buy", i, j)] = -1
                r[idx("sell", i, j)] = 1
                if j == 0:
                    held = 1 if (current_squad and players.player_id[i] in current_squad) else 0
                    cons.append(LinearConstraint(r, held, held))
                else:
                    r[idx("squad", i, j - 1)] = -1
                    cons.append(LinearConstraint(r, 0, 0))

            # Transfers used this week, against the bank.
            r = row()
            for i in range(n):
                r[idx("buy", i, j)] = 1
            r[hits_at + j] = -1
            r[banked_at + j] = -1
            cons.append(LinearConstraint(r, -np.inf, 0))

            # A hit has to correspond to an actual transfer.
            r = row()
            r[hits_at + j] = 1
            for i in range(n):
                r[idx("buy", i, j)] = -1
            cons.append(LinearConstraint(r, -np.inf, 0))

            # Bank evolution. What depletes the bank is *free* transfers used,
            # which is buys minus the ones paid for with a hit - not total buys.
            # Omitting the hits term makes a fifteen-transfer wildcard drive the
            # bank to -13 and the whole program infeasible.
            #
            #   banked_{j+1} <= banked_j - (buys_j - hits_j) + 1
            #
            # Expressed as an inequality with a small positive weight on banked
            # in the objective, which is exact here because banking is never
            # penalised, so the solver has no reason to under-report it.
            if j + 1 < w:
                r = row()
                r[banked_at + j + 1] = 1
                r[banked_at + j] = -1
                r[hits_at + j] = -1
                for i in range(n):
                    r[idx("buy", i, j)] = 1
                cons.append(LinearConstraint(r, -np.inf, 1))

        # Opening bank.
        r = row()
        r[banked_at] = 1
        cons.append(LinearConstraint(r, free_transfers, free_transfers))

        lower = np.zeros(total_vars)
        upper = np.ones(total_vars)
        upper[banked_at:banked_at + w] = self.max_banked
        upper[hits_at:hits_at + w] = self.squad_size

        integrality = np.ones(total_vars)

        result = milp(c=c, constraints=cons, integrality=integrality,
                      bounds=Bounds(lower, upper))
        if not result.success:
            raise RuntimeError(f"No feasible plan: {result.message}")

        x = np.round(result.x).astype(int)
        rows = []
        for j, gw in enumerate(gameweeks):
            for i in range(n):
                # Sold players are, by definition, no longer in that week's
                # squad - but "who to sell" is half of a transfer plan, so they
                # are reported alongside it rather than silently dropped.
                in_squad = bool(x[idx("squad", i, j)])
                sold = bool(x[idx("sell", i, j)])
                if in_squad or sold:
                    rows.append({
                        "gameweek": gw,
                        "player_id": players.player_id[i],
                        "position": position[i],
                        "team_id": team[i],
                        "price": price[i],
                        "xp": value[i, j],
                        "in_squad": in_squad,
                        "starting": bool(x[idx("start", i, j)]),
                        "is_captain": bool(x[idx("captain", i, j)]),
                        "bought": bool(x[idx("buy", i, j)]),
                        "sold": sold,
                    })
        plan = pd.DataFrame(rows)
        summary = pd.DataFrame({
            "gameweek": gameweeks,
            "banked_transfers": x[banked_at:banked_at + w],
            "hits": x[hits_at:hits_at + w],
        })
        return plan, summary
