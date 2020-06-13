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
            echo:
                type: boolean
        required:
            - engine
    finder:
        type: object
        properties:
            buffer:
                type: string
            storage:
                type: string
            blacklist:
                anyOf:
                    - type: array
                      items:
                          types: string
                    - type: "null"
            actions:
                type: object
                properties:
                    standard:
                        type: string
                    alternative:
                        type: string
            search_method:
                type: string
            pause:
                type: integer
                minimum: 0
        required:
            - buffer
            - storage
required:
    - database
    - finder
"""

ingester = """
type: object
properties:
    database:
        type: object
        properties:
            engine:
                type: string
            echo:
                type: boolean
        required:
            - engine
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