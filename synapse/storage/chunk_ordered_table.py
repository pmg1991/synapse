import math
import logging
import sys

from collections import namedtuple
from fractions import Fraction

from synapse.storage.engines import PostgresEngine


logger = logging.getLogger(__name__)


class FakeFrac(namedtuple("FakeFrac", ("numerator", "denominator"))):
    def __float__(self):
        return float(self.numerator) / float(self.denominator)

    def to_fraction(self):
        return Fraction(self.numerator, self.denominator)


def farey_function(n):
    a, b, c, d = 0, 1, 1, n
    yield FakeFrac(a, b)
    while c <= n:
        k = int((n + b) / d)
        a, b, c, d = c, d, (k * c - a), (k * d - b)
        yield FakeFrac(a, b)


def approximate_n(length):
    return int(math.ceil(math.sqrt(length * math.pi ** 2 / 3)))


def mediant(a, b):
    if (a >=0 and b >= 0) or (a <= 0 and b <= 0):
        return Fraction(
            a.numerator + b.numerator,
            a.denominator + b.denominator,
        )
    return Fraction(0, 1)


class OrderedChunkTable(object):
    def __init__(self, database_engine,
                 min_denominator=10000,
                 max_denominator=1000000):
        self.database_engine = database_engine

        self.min_denominator = min_denominator
        self.max_denominator = max_denominator

        self.rebalances = 0  # Replace with metrics

    def insert(self, txn, room_id, node_id, order=None):
        if order is None:
            txn.execute("""
                SELECT numerator, denominator
                FROM chunk_linearized
                WHERE room_id = ?
                ORDER BY rational DESC
                LIMIT 1
            """, (room_id,))

            row = txn.fetchone()
            if row:
                order = Fraction(*row) + 1
            else:
                order = Fraction(1, 1)

        sql = """
            INSERT INTO chunk_linearized
            (chunk_id, room_id, numerator, denominator, rational)
            VALUES (?, ?, ?, ?, ?)
        """
        txn.execute(sql, (
            node_id, room_id, order.numerator, order.denominator, float(order),
        ))

        self._rebalance(txn, room_id, node_id, order)

    def delete(self, txn, node_id):
        sql = """
            DELETE FROM chunk_linearized WHERE chunk_id = ?
        """
        txn.execute(sql, (node_id,))

    def get_order(self, txn, node_id):
        sql = """
            SELECT numerator, denominator FROM chunk_linearized WHERE chunk_id = ?
        """

        txn.execute(sql, (node_id,))
        row = txn.fetchone()
        if row:
            return Fraction(*row)
        return None

    def get_min_order(self, txn, room_id):
        txn.execute("""
            SELECT numerator, denominator
            FROM chunk_linearized
            WHERE room_id = ?
            ORDER BY rational ASC
            LIMIT 1
        """, (room_id,))

        row = txn.fetchone()
        if row:
            return Fraction(*row)
        else:
            return Fraction(1, 1)

    def get_prev(self, txn, room_id, node_id):
        order = self.get_order(txn, node_id)

        sql = """
            SELECT chunk_id FROM chunk_linearized
            WHERE rational < ? AND room_id = ?
            ORDER BY rational DESC
            LIMIT 1
        """

        txn.execute(sql, (float(order), room_id,))

        row = txn.fetchone()
        if row:
            return row[0]
        return None

    def get_next(self, txn, room_id, node_id):
        order = self.get_order(txn, node_id)

        sql = """
            SELECT chunk_id FROM chunk_linearized
            WHERE rational > ? AND room_id = ?
            ORDER BY rational ASC
            LIMIT 1
        """

        txn.execute(sql, (float(order), room_id,))

        row = txn.fetchone()
        if row:
            return row[0]
        return None

    def get_nodes_with_smaller_denominator(self, txn, room_id, node_id):
        order = self.get_order(txn, node_id)

        sql = """
            SELECT numerator, denominator
            FROM chunk_linearized
            WHERE rational < ? AND denominator < ? AND room_id = ?
            ORDER BY rational DESC
            LIMIT 1
        """

        txn.execute(sql, (float(order), self.min_denominator, room_id,))
        row = txn.fetchone()
        if row:
            low_order = Fraction(*row)
        else:
            low_order = Fraction(-sys.maxint, 1)

        sql = """
            SELECT COALESCE(numerator, 1), COALESCE(denominator, 0)
            FROM chunk_linearized
            WHERE rational > ? AND denominator < ? AND room_id = ?
            ORDER BY rational ASC
            LIMIT 1
        """

        txn.execute(sql, (float(order), self.min_denominator, room_id))

        row = txn.fetchone()
        if row:
            high_order = Fraction(*row)
        else:
            sql = """
                SELECT cast(rational + 2 as int), 1 FROM chunk_linearized
                WHERE room_id = ?
                ORDER BY rational DESC
                LIMIT 1
            """
            txn.execute(sql, (room_id,))
            high_order = Fraction(*txn.fetchone())

        sql = """
            SELECT count(*) FROM chunk_linearized
            WHERE rational >= ? AND rational <= ? AND room_id = ?
        """
        txn.execute(sql, (float(low_order), float(high_order), room_id,))
        cnt, = txn.fetchone()

        return low_order, high_order, cnt

    def _update_nodes(self, txn, room_id, low_order, high_order, farey_level):
        sql = """
            SELECT chunk_id, rational
            FROM chunk_linearized
            WHERE rational >= ? AND room_id = ?
            ORDER BY rational ASC
            LIMIT 1
        """
        txn.execute(sql, (float(low_order), room_id,))
        low_id, rational = txn.fetchone()

        diff = high_order - low_order

        with_sql = """
            WITH RECURSIVE farey_function(num, dem, c, d, new_r, id, prev_r) AS (
                VALUES (0, 1, 1, ?, 0.0, ?, ?)
                UNION ALL
                SELECT
                    c, d,
                    (((? + dem)/d) * c - num), (((? + dem)/d) * d - dem),
                    (c * 1.0) / (d * 1.0),
                    t.id, t.rational
                FROM farey_function
                INNER JOIN (
                    SELECT chunk_id AS id, rational,
                        (   SELECT chunk_id FROM chunk_linearized AS c
                            WHERE c.rational < chunk_linearized.rational
                            ORDER BY c.rational DESC
                            LIMIT 1
                        ) AS chunk_id
                    FROM chunk_linearized
                    WHERE room_id = ?
                ) AS t
                    ON chunk_id = farey_function.id
                WHERE c <= ?
            )
        """

        if isinstance(self.database_engine, PostgresEngine):
            raise NotImplementedError()
        else:
            sql = with_sql + """
                SELECT num, dem, id FROM farey_function
            """
            txn.execute(sql, (
                farey_level, low_id, rational,
                farey_level, farey_level,
                room_id,
                farey_level,
            ))

            rows = []
            for num, denom, chunk_id in txn:
                f = Fraction(num, denom)
                f = low_order + diff * f
                rows.append((f.numerator, f.denominator, float(f), chunk_id))

            sql = """
                UPDATE chunk_linearized SET numerator = ?, denominator = ?, rational = ?
                WHERE chunk_id = ?
            """
            txn.executemany(sql, rows)

    def _rebalance(self, txn, room_id, node_id, order):
        if order.denominator < self.max_denominator:
            return

        self.rebalances += 1

        low_order, high_order, cnt = self.get_nodes_with_smaller_denominator(
            txn, room_id, node_id,
        )

        n = approximate_n(cnt)
        while True:
            orders = list(farey_function(n))
            if len(orders) >= cnt:
                break
            n += 1

        self._update_nodes(txn, room_id, low_order, high_order, n)

    def insert_before(self, txn, room_id, node_id, target_id):
        if target_id:
            target_order = self.get_order(txn, target_id)
            before_id = self.get_prev(txn, room_id, target_id)

            if before_id:
                before_order = self.get_order(txn, before_id)
            else:
                before_order = Fraction(0, 1)

            new_order = mediant(target_order, before_order)
        else:
            new_order = None

        self.insert(txn, room_id, node_id, new_order)

    def insert_after(self, txn, room_id, node_id, target_id):
        if target_id:
            target_order = self.get_order(txn, target_id)
            after_id = self.get_next(txn, room_id, target_id)
            if after_id:
                after_order = self.get_order(txn, after_id)
            else:
                after_order = FakeFrac(1, 0)

            new_order = mediant(target_order, after_order)
        else:
            new_order = mediant(Fraction(0,1), self.get_min_order(txn, room_id))

        self.insert(txn, room_id, node_id, new_order)


def add_edge_to_linear_chunk(store, txn, table, room_id, source, target):
    """Implements the Katriel-Bodlaender algorithm.

    See https://www.sciencedirect.com/science/article/pii/S0304397507006573
    """
    to_s = []
    from_t = []
    to_s_neighbours = []
    from_t_neighbours = []
    to_s_indegree = 0
    from_t_outdegree = 0
    s = source
    t = target

    while s and t:
        o_s = table.get_order(txn, s)
        o_t = table.get_order(txn, t)

        logger.info("s: %s (%s), t: %s (%s)", s, o_s, t, o_t)

        if o_s <= o_t:
            break

        m_s = to_s_indegree
        m_t = from_t_outdegree

        pe_s = store._simple_select_onecol_txn(
            txn,
            table="chunk_graph",
            keyvalues={"chunk_id": s},
            retcol="prev_id",
        )

        fe_t = store._simple_select_onecol_txn(
            txn,
            table="chunk_graph",
            keyvalues={"prev_id": t},
            retcol="chunk_id",
        )

        l_s = len(pe_s)
        l_t = len(fe_t)

        if m_s + l_s <= m_t + l_t:
            to_s.append(s)
            to_s_neighbours.extend(pe_s)
            to_s_indegree += l_s

            if to_s_neighbours:
                to_s_neighbours.sort(key=lambda a: table.get_order(txn, a))  # FIXME
                s = to_s_neighbours.pop()
            else:
                s = None

        if m_s + l_s >= m_t + l_t:
            from_t.append(t)
            from_t_neighbours.extend(fe_t)
            from_t_outdegree += l_t

            if from_t_neighbours:
                from_t_neighbours.sort(key=lambda a: -table.get_order(txn, a))  # FIXME
                t = from_t_neighbours.pop()
            else:
                t = None

    logger.info("Calculated s: %s and t: %s", s, t)


    if s is None:
        s = table.get_prev(txn, room_id, target)

    if t is None:
        t = table.get_next(txn, room_id, source)

    while to_s:
        s1 = to_s.pop()
        table.delete(txn, s1)
        table.insert_after(txn, room_id, s1, s)
        s = s1

    while from_t:
        t1 = from_t.pop()
        table.delete(txn, t1)
        table.insert_before(txn, room_id, t1, t)
        t = t1
