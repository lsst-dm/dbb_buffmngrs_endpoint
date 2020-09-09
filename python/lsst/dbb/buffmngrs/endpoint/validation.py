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


__all__ = ["finder", "ingester"]


finder = """
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
                        type: string
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
                    blacklist:
                        anyOf:
                            - type: array
                              items:
                                  types: string
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

ingester = """
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
                        type: string
                    event:
                        type: string
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
            blacklist:
                anyOf:
                    - type: array
                      items:
                         type: string
                      uniqueItems: true
                    - type: "null"
            whitelist:
                anyOf:
                    - type: array
                      items:
                         type: string
                      uniqueItems: true
                    - type: "null"
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
                                        root:
                                            type: string
                                        mode:
                                            type: string
                                    required:
                                        - root
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
                                        root:
                                            type: string
                                        instrument:
                                            type: string
                                        transfer:
                                            type: string
                                    required:
                                        - root
                                        - instrument
                required:
                    - name
                    - config
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
required:
    - database
    - ingester
"""
