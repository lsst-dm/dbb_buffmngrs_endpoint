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


__all__ = ["schema"]


schema = """
---
type: object
properties:
    database:
    finders:
        type: array
        items:
            type: object
            properties:
                buffer:
                    type: string
                storage:
                    type: string
                blacklist:
                    anyOf:
                        - type: null
                        - type: array
                          items:
                              type: string
                actions:
                    type: object
                    properties:
                        standard:
                            anyOf:
                                - type: string
                                - type: null
                        alternative:
                            anyOf:
                                - type: string
                                - type: null
                search_method:
                    type: string
                pause:
                    type: integer
                    minimum: 0
                required:
                    - buffer
                    - storage
    ingesters:
        type: array
        items:
            type: object
            properties:
                plugin:
                    type: object
                    properties:
                        name:
                            type: string
                        config:
                            type: object
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
                type: string
            format:
                type: string
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
        - ingesters
"""