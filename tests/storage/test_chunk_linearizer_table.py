# -*- coding: utf-8 -*-
# Copyright 2018 New Vector Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from twisted.internet import defer

import random
import tests.unittest
import tests.utils

from synapse.storage.chunk_ordered_table import OrderedChunkTable


class ChunkLinearizerStoreTestCase(tests.unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(ChunkLinearizerStoreTestCase, self).__init__(*args, **kwargs)

    @defer.inlineCallbacks
    def setUp(self):
        hs = yield tests.utils.setup_test_homeserver()
        self.store = hs.get_datastore()
        self.clock = hs.get_clock()

        self.table = OrderedChunkTable(self.store.database_engine, 50, 100)

    @defer.inlineCallbacks
    def test_simple_insert_fetch(self):
        room_id = "foo_room"

        def test_txn(txn):
            self.table.insert(txn, room_id, "A")
            self.table.insert_after(txn, room_id, "B", "A")
            self.table.insert_before(txn, room_id, "C", "A")

            sql = """
                SELECT chunk_id FROM chunk_linearized
                WHERE room_id = ?
                ORDER BY rational ASC
            """
            txn.execute(sql, (room_id,))

            ordered = [r for r, in txn]

            self.assertEqual(["C", "A", "B"], ordered)


        yield self.store.runInteraction("test", test_txn)

    @defer.inlineCallbacks
    def test_many_insert_fetch(self):
        room_id = "foo_room"

        def test_txn(txn):
            nodes = [(i, "node_%d" % (i,)) for i in xrange(1, 100)]
            expected = [n for _, n in nodes]

            already_inserted = []

            random.shuffle(nodes)
            while nodes:
                i, node_id = nodes.pop()
                if not already_inserted:
                    self.table.insert(txn, room_id, node_id)
                else:
                    for j, target_id in already_inserted:
                        if j > i:
                            break

                    if j < i:
                        self.table.insert_after(txn, room_id, node_id, target_id)
                    else:
                        self.table.insert_before(txn, room_id, node_id, target_id)

                already_inserted.append((i, node_id))
                already_inserted.sort()

            sql = """
                SELECT chunk_id FROM chunk_linearized
                WHERE room_id = ?
                ORDER BY rational ASC
            """
            txn.execute(sql, (room_id,))

            ordered = [r for r, in txn]

            self.assertEqual(expected, ordered)


        yield self.store.runInteraction("test", test_txn)
