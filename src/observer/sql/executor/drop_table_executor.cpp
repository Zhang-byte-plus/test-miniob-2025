/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/6/13.
//

#include "sql/executor/drop_table_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/drop_table_stmt.h"
#include "storage/db/db.h"

RC DropTableExecutor::execute(SQLStageEvent *sql_event)
{
  try {
    Stmt    *stmt    = sql_event->stmt();
    Session *session = sql_event->session_event()->session();
    
    if (nullptr == stmt || nullptr == session) {
      LOG_ERROR("Invalid parameters in DropTableExecutor::execute");
      return RC::INTERNAL;
    }

    DropTableStmt *drop_table_stmt = static_cast<DropTableStmt *>(stmt);
    if (nullptr == drop_table_stmt) {
      LOG_ERROR("Invalid drop table statement");
      return RC::INTERNAL;
    }

    const char *table_name = drop_table_stmt->table_name().c_str();
    if (nullptr == table_name || table_name[0] == '\0') {
      LOG_ERROR("Invalid table name");
      return RC::INTERNAL;
    }

    Database *db = session->get_current_db();
    if (nullptr == db) {
      LOG_ERROR("No current database selected");
      return RC::INTERNAL;
    }

    RC rc = db->drop_table(table_name);
    return rc;
  } catch (const std::exception &e) {
    LOG_ERROR("Exception occurred during drop table execution: %s", e.what());
    return RC::INTERNAL;
  } catch (...) {
    LOG_ERROR("Unknown exception occurred during drop table execution");
    return RC::INTERNAL;
  }
}