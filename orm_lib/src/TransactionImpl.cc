/**
 *
 *  TransactionImpl.cc
 *  An Tao
 *
 *  Copyright 2018, An Tao.  All rights reserved.
 *  https://github.com/an-tao/drogon
 *  Use of this source code is governed by a MIT license
 *  that can be found in the License file.
 *
 *  Drogon
 *
 */

#include "TransactionImpl.h"
#include "../../lib/src/TaskTimeoutFlag.h"
#if USE_SQLITE3
#include "sqlite3_impl/Sqlite3Connection.h"
#endif
#include <string_view>
#include <trantor/utils/Logger.h>

using namespace drogon::orm;
using namespace drogon;

namespace
{
#if USE_SQLITE3
void rollbackFailedSqliteCommit(const DbConnectionPtr &conn,
                                const std::function<void()> &usedUpCallback,
                                const std::function<void(bool)> &commitCallback)
{
    // SQLite can leave the transaction open after COMMIT fails. Do not return
    // this connection to the pool when the failed command becomes idle.
    conn->setIdleCallback([]() {});
    conn->loop()->queueInLoop([conn, usedUpCallback, commitCallback]() {
        auto sqlite = std::static_pointer_cast<Sqlite3Connection>(conn);
        auto finish = [sqlite, usedUpCallback, commitCallback](bool reusable) {
            // Let the failed statement and its callbacks finish before closing
            // the connection or allowing the pool to dispatch another query.
            sqlite->loop()->queueInLoop(
                [sqlite, usedUpCallback, commitCallback, reusable]() {
                    if (!reusable)
                        sqlite->invalidate();
                    if (commitCallback)
                        commitCallback(false);
                    if (reusable && usedUpCallback)
                        usedUpCallback();
                });
        };
        // Some COMMIT errors already roll back the entire transaction.
        if (!sqlite->hasActiveTransaction())
        {
            finish(true);
            return;
        }
        conn->execSql(
            "rollback",
            0,
            {},
            {},
            {},
            [sqlite, finish](const Result &) {
                LOG_TRACE << "Transaction rolled back after failed commit";
                finish(!sqlite->hasActiveTransaction());
            },
            [sqlite, finish](const std::exception_ptr &) {
                LOG_ERROR << "Transaction rollback after failed commit failed";
                finish(!sqlite->hasActiveTransaction());
            });
    });
}
#endif

/// Sends the implicit COMMIT of a transaction that was released without an
/// explicit commit or rollback, once its connection is idle.
void commitOnIdleConnection(const DbConnectionPtr &conn,
                            ClientType type,
                            const std::function<void()> &ucb,
                            const std::function<void(bool)> &commitCb)
{
    if (conn->isWorking())
    {
        // The last reference to a transaction can be released inside a result
        // callback while the connection is still busy with its own follow-up
        // command, such as the DEALLOCATE that evicts a cached prepared
        // statement. COMMIT sent now would fail with "another command is
        // already in progress" and the transaction's writes would be lost, so
        // wait until the connection is idle. COMMIT is not sent from inside
        // the idle callback because sending it replaces that callback while
        // it runs.
        auto loop = conn->loop();
        conn->setIdleCallback([conn, type, ucb, commitCb, loop]() {
            loop->queueInLoop([conn, type, ucb, commitCb]() {
                commitOnIdleConnection(conn, type, ucb, commitCb);
            });
        });
        return;
    }
    conn->setIdleCallback([ucb]() {
        if (ucb)
            ucb();
    });
    conn->execSql(
        "commit",
        0,
        {},
        {},
        {},
        [commitCb](const Result &) {
            LOG_TRACE << "Transaction committed!";
            if (commitCb)
            {
                commitCb(true);
            }
        },
        [conn, type, ucb, commitCb](const std::exception_ptr &ePtr) {
            try
            {
                std::rethrow_exception(ePtr);
            }
            catch (const DrogonDbException &e)
            {
                LOG_ERROR << "Transaction submission failed:"
                          << e.base().what();
#if USE_SQLITE3
                if (type == ClientType::Sqlite3)
                {
                    rollbackFailedSqliteCommit(conn, ucb, commitCb);
                    return;
                }
#endif
                if (commitCb)
                {
                    commitCb(false);
                }
            }
        });
}
}  // namespace

TransactionImpl::TransactionImpl(ClientType type,
                                 const DbConnectionPtr &connPtr,
                                 std::function<void(bool)> commitCallback,
                                 std::function<void()> usedUpCallback,
                                 TransactionType transType)
    : connectionPtr_(connPtr),
      usedUpCallback_(std::move(usedUpCallback)),
      loop_(connPtr->loop()),
      commitCallback_(std::move(commitCallback)),
      transactionType_(transType)
{
    type_ = type;
}

TransactionImpl::~TransactionImpl()
{
    LOG_TRACE << "Destruct";
    assert(sqlCmdBuffer_.empty());
    if (!isCommitOrRollbackRequested_ && !isCommitedOrRolledback_)
    {
        auto loop = connectionPtr_->loop();
        loop->queueInLoop([conn = connectionPtr_,
                           type = type_,
                           ucb = std::move(usedUpCallback_),
                           commitCb = std::move(commitCallback_)]() {
            commitOnIdleConnection(conn, type, ucb, commitCb);
        });
    }
    else
    {
        if (usedUpCallback_)
        {
            usedUpCallback_();
        }
    }
}

void TransactionImpl::execSqlInLoop(
    std::string_view &&sql,
    size_t paraNum,
    std::vector<const char *> &&parameters,
    std::vector<int> &&length,
    std::vector<int> &&format,
    ResultCallback &&rcb,
    std::function<void(const std::exception_ptr &)> &&exceptCallback)
{
    loop_->assertInLoopThread();
    if (!isCommitOrRollbackRequested_ && !isCommitedOrRolledback_)
    {
        if (timeout_ > 0.0)
        {
            execSqlInLoopWithTimeout(std::move(sql),
                                     paraNum,
                                     std::move(parameters),
                                     std::move(length),
                                     std::move(format),
                                     std::move(rcb),
                                     std::move(exceptCallback));
            return;
        }
        auto thisPtr = shared_from_this();
        if (!isWorking_)
        {
            isWorking_ = true;
            thisPtr_ = thisPtr;
            connectionPtr_->execSql(std::move(sql),
                                    paraNum,
                                    std::move(parameters),
                                    std::move(length),
                                    std::move(format),
                                    std::move(rcb),
                                    [exceptCallback,
                                     thisPtr](const std::exception_ptr &ePtr) {
                                        thisPtr->rollback();
                                        if (exceptCallback)
                                            exceptCallback(ePtr);
                                    });
        }
        else
        {
            // push sql cmd to buffer;
            auto cmdPtr = std::make_shared<SqlCmd>();
            cmdPtr->sql_ = std::move(sql);
            cmdPtr->parametersNumber_ = paraNum;
            cmdPtr->parameters_ = std::move(parameters);
            cmdPtr->lengths_ = std::move(length);
            cmdPtr->formats_ = std::move(format);
            cmdPtr->callback_ = std::move(rcb);
            cmdPtr->exceptionCallback_ = std::move(exceptCallback);
            cmdPtr->thisPtr_ = thisPtr;
            thisPtr->sqlCmdBuffer_.push_back(std::move(cmdPtr));
        }
    }
    else
    {
        auto exceptPtr = std::make_exception_ptr(TransactionRollback(
            "The transaction has already been committed or rolled back"));
        exceptCallback(exceptPtr);
    }
}

void TransactionImpl::rollback()
{
    auto thisPtr = shared_from_this();

    loop_->runInLoop([thisPtr]() {
        if (thisPtr->isCommitedOrRolledback_)
            return;
        if (thisPtr->isCommitOrRollbackRequested_)
        {
            // A rollback may only overtake a commit that has not been sent
            if (!thisPtr->cancelPendingCommit())
                return;
        }
        else
        {
            thisPtr->isCommitOrRollbackRequested_ = true;
            thisPtr->isRolledBack_ = true;
        }
        if (thisPtr->isWorking_)
        {
            // push sql cmd to buffer;
            auto cmdPtr = std::make_shared<SqlCmd>();
            cmdPtr->sql_ = "rollback";
            cmdPtr->parametersNumber_ = 0;
            cmdPtr->callback_ = [thisPtr](const Result &) {
                LOG_DEBUG << "Transaction roll back!";
                thisPtr->isCommitedOrRolledback_ = true;
            };
            cmdPtr->exceptionCallback_ = [thisPtr](const std::exception_ptr &) {
                // clearupCb();
                thisPtr->isCommitedOrRolledback_ = true;
                LOG_ERROR << "Transaction roll back error";
            };
            cmdPtr->isRollbackCmd_ = true;
            // Rollback cmd should be executed firstly, so we push it in front
            // of the list
            thisPtr->sqlCmdBuffer_.push_front(std::move(cmdPtr));
            return;
        }
        thisPtr->isWorking_ = true;
        thisPtr->thisPtr_ = thisPtr;
        thisPtr->connectionPtr_->execSql(
            "rollback",
            0,
            {},
            {},
            {},
            [thisPtr](const Result &) {
                LOG_TRACE << "Transaction roll back!";
                thisPtr->isCommitedOrRolledback_ = true;
                // clearupCb();
            },
            [thisPtr](const std::exception_ptr &) {
                // clearupCb();
                LOG_ERROR << "Transaction roll back error";
                thisPtr->isCommitedOrRolledback_ = true;
            });
    });
}

void TransactionImpl::commitSync()
{
    if (loop_->isInLoopThread())
        throw UsageError(
            "commitSync() would block the transaction's own event loop");
    std::promise<std::exception_ptr> promise;
    auto future = promise.get_future();
    commitAsyncImpl([&promise](const std::exception_ptr &exception) {
        promise.set_value(exception);
    });
    if (auto exception = future.get())
        std::rethrow_exception(exception);
}

void TransactionImpl::commitAsyncImpl(
    std::function<void(const std::exception_ptr &)> &&callback)
{
    auto thisPtr = shared_from_this();
    loop_->runInLoop([thisPtr, callback = std::move(callback)]() mutable {
        if (thisPtr->isCommitOrRollbackRequested_ ||
            thisPtr->isCommitedOrRolledback_)
        {
            // A rollback request without a pending commit means the
            // transaction is (being) rolled back; anything else is a second
            // commit request.
            auto exception =
                thisPtr->isRolledBack_
                    ? std::make_exception_ptr(TransactionRollback(
                          "The transaction has been rolled back"))
                    : std::make_exception_ptr(
                          UsageError("Commit was already requested"));
            if (callback)
                callback(exception);
            return;
        }

        thisPtr->isCommitOrRollbackRequested_ = true;
        thisPtr->explicitCommitCallback_ = std::move(callback);
        auto cmdPtr = std::make_shared<SqlCmd>();
        cmdPtr->sql_ = "commit";
        cmdPtr->parametersNumber_ = 0;
        cmdPtr->callback_ = [thisPtr](const Result &) {
            LOG_TRACE << "Transaction committed!";
            thisPtr->isCommitedOrRolledback_ = true;
            thisPtr->finishCommit(nullptr);
        };
        cmdPtr->exceptionCallback_ = [thisPtr](const std::exception_ptr &ePtr) {
            try
            {
                std::rethrow_exception(ePtr);
            }
            catch (const DrogonDbException &e)
            {
                LOG_ERROR << "Transaction submission failed:"
                          << e.base().what();
            }
            thisPtr->isCommitedOrRolledback_ = true;
#if USE_SQLITE3
            if (thisPtr->type_ == ClientType::Sqlite3)
            {
                // The connection is released once the failed commit has been
                // rolled back, not when the failed command becomes idle.
                auto usedUpCallback = std::move(thisPtr->usedUpCallback_);
                thisPtr->usedUpCallback_ = std::function<void()>();
                rollbackFailedSqliteCommit(thisPtr->connectionPtr_,
                                           usedUpCallback,
                                           [thisPtr, ePtr](bool) {
                                               thisPtr->finishCommit(ePtr);
                                           });
                return;
            }
#endif
            thisPtr->finishCommit(ePtr);
        };
        cmdPtr->isCommitCmd_ = true;
        cmdPtr->thisPtr_ = thisPtr;
        thisPtr->sqlCmdBuffer_.push_back(std::move(cmdPtr));

        if (!thisPtr->isWorking_)
        {
            thisPtr->isWorking_ = true;
            thisPtr->execNewTask();
        }
    });
}

bool TransactionImpl::cancelPendingCommit()
{
    // No SQL is accepted after a commit request, so a pending commit is always
    // the last buffered command.
    if (sqlCmdBuffer_.empty() || !sqlCmdBuffer_.back()->isCommitCmd_)
        return false;
    sqlCmdBuffer_.pop_back();
    isRolledBack_ = true;
    // The commit was never sent, so only the explicit commit callback is
    // notified. It is deferred so that the failure which caused the
    // cancellation is reported first.
    if (explicitCommitCallback_)
    {
        loop_->queueInLoop([callback = std::move(explicitCommitCallback_)]() {
            callback(std::make_exception_ptr(TransactionRollback(
                "The transaction was rolled back before COMMIT was sent")));
        });
        explicitCommitCallback_ = nullptr;
    }
    return true;
}

void TransactionImpl::finishCommit(const std::exception_ptr &exception)
{
    if (commitCallback_)
        commitCallback_(exception == nullptr);
    if (explicitCommitCallback_)
    {
        auto callback = std::move(explicitCommitCallback_);
        explicitCommitCallback_ = nullptr;
        callback(exception);
    }
}

void TransactionImpl::execNewTask()
{
    loop_->assertInLoopThread();
    thisPtr_.reset();
    if (!isWorking_)
        return;
    assert(isWorking_);
    if (!isCommitedOrRolledback_)
    {
        auto thisPtr = shared_from_this();
        if (!sqlCmdBuffer_.empty())
        {
            auto cmd = std::move(sqlCmdBuffer_.front());
            sqlCmdBuffer_.pop_front();
            auto conn = connectionPtr_;
            conn->execSql(
                std::move(cmd->sql_),
                cmd->parametersNumber_,
                std::move(cmd->parameters_),
                std::move(cmd->lengths_),
                std::move(cmd->formats_),
                [callback = std::move(cmd->callback_), cmd, thisPtr](
                    const Result &r) {
                    if (cmd->isRollbackCmd_ || cmd->isCommitCmd_)
                    {
                        thisPtr->isCommitedOrRolledback_ = true;
                    }
                    if (callback)
                        callback(r);
                },
                [cmd, thisPtr](const std::exception_ptr &ePtr) {
                    if (!cmd->isRollbackCmd_ && !cmd->isCommitCmd_)
                        thisPtr->rollback();
                    else
                    {
                        thisPtr->isCommitedOrRolledback_ = true;
                    }
                    if (cmd->exceptionCallback_)
                        cmd->exceptionCallback_(ePtr);
                });
            return;
        }
        isWorking_ = false;
    }
    else
    {
        isWorking_ = false;
        failBufferedCommands(std::make_exception_ptr(
            TransactionRollback("The transaction has been rolled back")));
        releaseConnection();
    }
}

void TransactionImpl::releaseConnection()
{
    if (usedUpCallback_)
    {
        usedUpCallback_();
        usedUpCallback_ = std::function<void()>();
    }
}

void TransactionImpl::failBufferedCommands(const std::exception_ptr &ePtr)
{
    cancelPendingCommit();
    std::list<SqlCmdPtr> pendingCmds;
    pendingCmds.swap(sqlCmdBuffer_);
    for (auto &cmd : pendingCmds)
    {
        cmd->thisPtr_.reset();
        if (cmd->exceptionCallback_)
        {
            cmd->exceptionCallback_(ePtr);
        }
    }
}

const char *TransactionImpl::beginSql() const noexcept
{
    if (type_ != ClientType::Sqlite3)
        return "begin";

    switch (transactionType_)
    {
        case TransactionType::Immediate:
            return "begin immediate";
        case TransactionType::Exclusive:
            return "begin exclusive";
        default:
            return "begin";
    }
}

void TransactionImpl::doBegin()
{
    loop_->queueInLoop([thisPtr = shared_from_this()]() {
        std::weak_ptr<TransactionImpl> weakPtr = thisPtr;
        thisPtr->connectionPtr_->setIdleCallback([weakPtr]() {
            auto thisPtr = weakPtr.lock();
            if (!thisPtr)
                return;
            thisPtr->execNewTask();
        });
        assert(!thisPtr->isWorking_);
        assert(!thisPtr->isCommitedOrRolledback_);
        thisPtr->isWorking_ = true;
        thisPtr->thisPtr_ = thisPtr;
        thisPtr->connectionPtr_->execSql(
            thisPtr->beginSql(),
            0,
            {},
            {},
            {},
            [](const Result &) { LOG_TRACE << "Transaction begin!"; },
            [thisPtr](const std::exception_ptr &ePtr) {
                try
                {
                    std::rethrow_exception(ePtr);
                }
                catch (const std::exception &e)
                {
                    LOG_ERROR << "Error occurred in transaction begin:"
                              << e.what();
                }
                thisPtr->isCommitedOrRolledback_ = true;
                thisPtr->isRolledBack_ = true;
                thisPtr->isWorking_ = false;
                thisPtr->thisPtr_.reset();
                thisPtr->failBufferedCommands(std::make_exception_ptr(
                    TransactionRollback("Transaction begin failed, cannot "
                                        "execute queued SQL")));
                thisPtr->releaseConnection();
            });
    });
}

void TransactionImpl::execSqlInLoopWithTimeout(
    std::string_view &&sql,
    size_t paraNum,
    std::vector<const char *> &&parameters,
    std::vector<int> &&length,
    std::vector<int> &&format,
    ResultCallback &&rcb,
    std::function<void(const std::exception_ptr &)> &&ecb)
{
    auto thisPtr = shared_from_this();
    std::weak_ptr<TransactionImpl> weakPtr = thisPtr;
    auto commandPtr = std::make_shared<std::weak_ptr<SqlCmd>>();
    auto ecpPtr =
        std::make_shared<std::function<void(const std::exception_ptr &)>>(
            std::move(ecb));
    auto timeoutFlagPtr = std::make_shared<drogon::TaskTimeoutFlag>(
        loop_,
        std::chrono::duration<double>(timeout_),
        [commandPtr, weakPtr, ecpPtr]() {
            auto thisPtr = weakPtr.lock();
            if (!thisPtr)
                return;
            auto cmdPtr = (*commandPtr).lock();
            if (cmdPtr)
            {
                for (auto iter = thisPtr->sqlCmdBuffer_.begin();
                     iter != thisPtr->sqlCmdBuffer_.end();
                     ++iter)
                {
                    if (cmdPtr == *iter)
                    {
                        thisPtr->sqlCmdBuffer_.erase(iter);
                        break;
                    }
                }
            }
            thisPtr->rollback();
            if (*ecpPtr)
            {
                (*ecpPtr)(std::make_exception_ptr(
                    TimeoutError("SQL execution timeout")));
            }
        });
    auto resultCallback = [rcb = std::move(rcb),
                           timeoutFlagPtr](const drogon::orm::Result &result) {
        if (timeoutFlagPtr->done())
            return;
        rcb(result);
    };
    if (!isWorking_)
    {
        isWorking_ = true;
        thisPtr_ = thisPtr;
        connectionPtr_->execSql(std::move(sql),
                                paraNum,
                                std::move(parameters),
                                std::move(length),
                                std::move(format),
                                std::move(resultCallback),
                                [ecpPtr, timeoutFlagPtr, thisPtr](
                                    const std::exception_ptr &ePtr) {
                                    thisPtr->rollback();
                                    if (timeoutFlagPtr->done())
                                        return;
                                    if (*ecpPtr)
                                    {
                                        (*ecpPtr)(ePtr);
                                    }
                                });
    }
    else
    {
        // push sql cmd to buffer;
        auto cmdPtr = std::make_shared<SqlCmd>();
        cmdPtr->sql_ = std::move(sql);
        cmdPtr->parametersNumber_ = paraNum;
        cmdPtr->parameters_ = std::move(parameters);
        cmdPtr->lengths_ = std::move(length);
        cmdPtr->formats_ = std::move(format);
        cmdPtr->callback_ = std::move(resultCallback);
        cmdPtr->exceptionCallback_ =
            [ecpPtr, timeoutFlagPtr](const std::exception_ptr &ePtr) {
                if (timeoutFlagPtr->done())
                    return;
                if (*ecpPtr)
                {
                    (*ecpPtr)(ePtr);
                }
            };

        cmdPtr->thisPtr_ = thisPtr;
        thisPtr->sqlCmdBuffer_.push_back(cmdPtr);
        *commandPtr = cmdPtr;
    }
    timeoutFlagPtr->runTimer();
}
