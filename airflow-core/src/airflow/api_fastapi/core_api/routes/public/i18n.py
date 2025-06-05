# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os

from fastapi import HTTPException
from fastapi.responses import FileResponse

from airflow.api_fastapi.common.router import AirflowRouter

i18n_router = AirflowRouter(tags=["i18n"])

@i18n_router.get("/i18n/{lang}/{ns}.json", response_class=FileResponse, summary="Get translation file")
async def get_translation(lang: str, ns: str):
    """
    Return the translation file for the requested language and namespace.

    - lang: Language code (e.g., en, ko, nl, pl, zh-TW)
    - ns: Namespace (e.g., common, dashboard, dags, connections)
    """
    file_path = os.path.join(os.path.dirname(__file__), "../../i18n/locales", lang, f"{ns}.json")
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="Translation file not found")
    return FileResponse(file_path, media_type="application/json")

router = i18n_router
