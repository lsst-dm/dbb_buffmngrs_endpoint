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
            orms:
                type: object
                properties:
                    file:
                        type: string
                required:
                    - file
            echo:
                type: boolean
        required:
            - engine
            - orms
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
                        type: string
                        enum:
                            - Delete
                            - Move
                            - Noop
                    alternative:
                        type: string
                        enum:
                            - Delete
                            - Move
                            - Noop
            search:
                type: object
                properties:
                    method:
                        type: string
                        enum:
                            - scan
                            - parse
                    blacklist:
                        anyOf:
                            - type: array
                              items:
                                  types: string
                            - type: "null"
                    date:
                        type: string
                    timespan:
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
            orms:
                type: object
                properties:
                    file:
                        type: string
                    status:
                        type: string
                    attempt:
                        type: string
                required:
                    - file
                    - status
                    - attempt
            echo:
                type: boolean
        required:
            - engine
            - orms
    ingester:
        type: object
        properties:
            plugin:
                type: object
                properties:
                    name:
                        type: string
                    config:
                        type: object
                        properties:
                            root:
                                type: string
                        required:
                            - root
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
            pool_size:
                type: integer
                minimum: 1
            pause:
                type: integer
                minimum: 0
        required:
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