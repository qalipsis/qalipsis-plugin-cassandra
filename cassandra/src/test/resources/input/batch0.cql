/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:34:21', 'Car #1', 'Ignition on');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:34:21', 'Car #2', 'Ignition on');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:34:21', 'Truck #1', 'Ignition on');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:35:08', 'Car #1', 'Engine on');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:35:08', 'Car #2', 'Engine on');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:35:08', 'Truck #1', 'Engine on');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:36:08', 'Car #1', 'Driving');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:36:08', 'Car #2', 'Driving');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME)
VALUES (42, '2020-10-20T12:36:08', 'Truck #1', 'Driving');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME) VALUES (42, '2020-10-20T12:38:56', 'Car #1', 'Leaving office geofence');
INSERT INTO TRACKER (DUMMY_NODE_ID, EVENT_TIMESTAMP, DEVICE_NAME, EVENT_NAME) VALUES (42, '2020-10-20T12:38:56', 'Car #2', 'Leaving office geofence');