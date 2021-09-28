# This file is part of dbb_buffmngrs_endpoint.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""Descriptions of a valid configuration files.
"""


__all__ = ["BACKFILL", "FINDER", "INGESTER"]


FINDER = """
---
type: object
properties:
    database:
        type: object
        properties:
            engine:
                type: string
            tablenames:
                type: object
                properties:
                    file:
                        type: object
                        properties:
                            schema:
                                anyOf:
                                    - type: string
                                    - type: "null"
                            table:
                                type: string
                        required:
                            - table
                required:
                    - file
            echo:
                type: boolean
            pool_class:
                type: string
        required:
            - engine
            - tablenames
    finder:
        type: object
        properties:
            source:
                type: string
            storage:
                type: string
            actions:
                type: object
                properties:
                    standard:
                        anyOf:
                            - type: string
                              enum:
                                - Delete
                                - Move
                                - Noop
                            - type: "null"
                    alternative:
                        anyOf:
                            - type: string
                              enum:
                                - Delete
                                - Move
                                - Noop
                            - type: "null"
            search:
                type: object
                properties:
                    method:
                        type: string
                        enum:
                            - scan
                            - parse_rsync_logs
                    exclude_list:
                        anyOf:
                            - type: array
                              items:
                                  types: string
                              uniqueItems: true
                            - type: "null"
                    date:
                        anyOf:
                            - type: string
                              format: date
                            - type: "null"
                    past_days:
                        type: integer
                        minimum: 0
                    future_days:
                        type: integer
                        minimum: 0
                    delay:
                        type: integer
                        minimum: 0
                required:
                    - method
            pause:
                type: integer
                minimum: 0
        required:
            - actions
            - search
            - source
            - storage
required:
    - database
    - finder
"""

INGESTER = """
---
type: object
properties:
    database:
        type: object
        properties:
            engine:
                type: string
            tablenames:
                type: object
                properties:
                    file:
                        type: object
                        properties:
                            schema:
                                anyOf:
                                    - type: string
                                    - type: "null"
                            table:
                                type: string
                        required:
                            - table
                    event:
                        type: object
                        properties:
                            schema:
                                anyOf:
                                    - type: string
                                    - type: "null"
                            table:
                                type: string
                        required:
                            - table
                required:
                    - file
                    - event
            echo:
                type: boolean
            pool_class:
                type: string
        required:
            - engine
            - tablenames
    ingester:
        type: object
        properties:
            storage:
                type: string
            plugin:
                type: object
                properties:
                    name:
                        type: string
                allOf:
                    -
                        if:
                            properties:
                                name:
                                    const: Gen2Ingest
                        then:
                            properties:
                                config:
                                    type: object
                                    properties:
                                        butler:
                                            type: object
                                            properties:
                                                root:
                                                    type: string
                                            required:
                                                - root
                                        ingest:
                                            type: object
                                            properties:
                                                create:
                                                    type: boolean
                                                dryrun:
                                                    type: boolean
                                                ignoreIngested:
                                                    type: boolean
                                                mode:
                                                    type: string
                                                    enum:
                                                        - copy
                                                        - link
                                                        - move
                                                        - skip
                                                task:
                                                    type: string
                                    required:
                                        - butler
                    -
                        if:
                            properties:
                                name:
                                    const: Gen3Ingest
                        then:
                            properties:
                                config:
                                    type: object
                                    properties:
                                        butler:
                                            type: object
                                            properties:
                                                root:
                                                    type: string
                                                collection:
                                                    anyOf:
                                                        - type: string
                                                        - type: "null"
                                            required:
                                                - root
                                        ingest:
                                            type: object
                                            properties:
                                                config:
                                                    anyOf:
                                                        - type: object
                                                        - type: "null"
                                                config_file:
                                                    anyOf:
                                                        - type: string
                                                        - type: "null"
                                                processes:
                                                    type: integer
                                                    minimum: 1
                                                transfer:
                                                    type: string
                                                    enum:
                                                        - auto
                                                        - link
                                                        - symlink
                                                        - hardlink
                                                        - copy
                                                        - move
                                                        - relsymlink
                                                        - direct
                                                task:
                                                    type: string
                                        visits:
                                            type: object
                                            properties:
                                                config_file:
                                                    anyOf:
                                                        - type: string
                                                        - type: "null"
                                                instrument:
                                                    type: string
                                                processes:
                                                    type: integer
                                                    minimum: 1
                                                task:
                                                    type: string
                                            required:
                                                - instrument
                                    required:
                                        - butler
                                        - ingest
                required:
                    - name
                    - config
            include_list:
                anyOf:
                    - type: array
                      items:
                          type: string
                      uniqueItems: true
                    - type: "null"
            exclude_list:
                anyOf:
                    - type: array
                      items:
                          type: string
                      uniqueItems: true
                    - type: "null"
            file_status:
                type: string
            batch_size:
                type: integer
                minimum: 1
            daemon:
                type: boolean
            num_threads:
                type: integer
                minimum: 1
            pause:
                type: integer
                minimum: 0
        required:
            - storage
            - plugin
    logging:
        type: object
        properties:
            file:
                anyOf:
                    - type: string
                    - type: "null"
            format:
                anyOf:
                    - type: string
                    - type: "null"
            level:
                type: string
                enum:
                    - DEBUG
                    - INFO
                    - WARNING
                    - ERROR
                    - CRITICAL
            rotate:
                anyOf:
                    - type: string
                    - enum:
                        - TIME
                        - SIZE
                    -type: "null"
            when:
                type: string
                enum:
                    - 'S'
                    - 'M'
                    - 'H'
                    - 'D'
                    - 'W0'
                    - 'W1'
                    - 'W2'
                    - 'W3'
                    - 'W4'
                    - 'W5'
                    - 'W6'
                    - 'midnight'
            interval:
                type: integer
            maxbytes:
                type: integer
            backup_count:
                type: integer
required:
    - database
    - ingester
"""

BACKFILL = """
---
type: object
properties:
    database:
        type: object
        properties:
            engine:
                type: string
            tablenames:
                type: object
                properties:
                    file:
                        type: object
                        properties:
                            schema:
                                anyOf:
                                    - type: string
                                    - type: "null"
                            table:
                                type: string
                        required:
                            - table
                    event:
                        type: object
                        properties:
                            schema:
                                anyOf:
                                    - type: string
                                    - type: "null"
                            table:
                                type: string
                        required:
                            - table
                required:
                    - file
                    - event
            echo:
                type: boolean
            pool_class:
                type: string
        required:
            - engine
            - tablenames
    backfill:
        type: object
        properties:
            storage:
                type: string
            sources:
                type: array
                items:
                    type: string
            search:
                type: object
                properties:
                    exclude_list:
                        anyOf:
                            - type: array
                              items:
                                 type: string
                              uniqueItems: true
                            - type: "null"
        required:
            - storage
            - sources
    logging:
        type: object
        properties:
            file:
                anyOf:
                    - type: string
                    - type: "null"
            format:
                anyOf:
                    - type: string
                    - type: "null"
            level:
                type: string
                enum:
                    - DEBUG
                    - INFO
                    - WARNING
                    - ERROR
                    - CRITICAL
            rotate:
                anyOf:
                    - type: string
                    - enum:
                        - TIME
                        - SIZE
                    -type: "null"
            when:
                type: string
                enum:
                    - 'S'
                    - 'M'
                    - 'H'
                    - 'D'
                    - 'W0'
                    - 'W1'
                    - 'W2'
                    - 'W3'
                    - 'W4'
                    - 'W5'
                    - 'W6'
                    - 'midnight'
            interval:
                type: integer
            maxbytes:
                type: integer
            backup_count:
                type: integer
required:
    - database
    - backfill
"""
