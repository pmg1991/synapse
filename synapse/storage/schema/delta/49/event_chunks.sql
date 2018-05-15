/* Copyright 2018 New Vector Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

ALTER TABLE events ADD COLUMN chunk_id BIGINT;

-- TODO: This should probably be done as a background update.
CREATE INDEX events_chunk ON events (room_id, chunk_id, topological_ordering, stream_ordering);

CREATE TABLE chunk_graph (
    chunk_id BIGINT NOT NULL,
    prev_id BIGINT NOT NULL
);

CREATE UNIQUE INDEX chunk_graph_id ON chunk_graph (chunk_id, prev_id);
CREATE INDEX chunk_graph_prev_id ON chunk_graph (prev_id);


CREATE TABLE chunk_backwards_extremities (
    chunk_id BIGINT NOT NULL,
    event_id TEXT NOT NULL
);

CREATE INDEX chunk_backwards_extremities_id ON chunk_backwards_extremities(chunk_id, event_id);
CREATE INDEX chunk_backwards_extremities_event_id ON chunk_backwards_extremities(event_id);


CREATE TABLE chunk_linearized (
    chunk_id BIGINT NOT NULL,
    room_id TEXT NOT NULL,
    numerator BIGINT NOT NULL,
    denominator BIGINT NOT NULL,
    rational DOUBLE PRECISION NOT NULL
);

CREATE UNIQUE INDEX chunk_linearized_id ON chunk_linearized (chunk_id);
CREATE INDEX chunk_linearized_rational ON chunk_linearized (room_id, rational);
