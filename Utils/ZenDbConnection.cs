using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;
using Zen.DbAccess.DatabaseSpeciffic;
using Zen.DbAccess.Enums;
using Zen.DbAccess.Interfaces;

namespace Zen.DbAccess.Utils;

public class ZenDbConnection : IZenDbConnection
{
    private DbConnection? _conn = null;
    private DbTransaction? _tx = null;
    private DbConnectionType _dbType;
    private IDbSpeciffic? _dbSpeciffic = null;
    private DbNamingConvention _dbNamingConvention = DbNamingConvention.SnakeCase;

    public ZenDbConnection(
        DbConnection conn,
        DbConnectionType dbType,
        IDbSpeciffic? dbSpeciffic,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        _conn = conn;
        _tx = null;
        _dbType = dbType;
        _dbSpeciffic = dbSpeciffic;
        _dbNamingConvention = dbNamingConvention;
    }

    public ZenDbConnection(
        DbConnection conn,
        DbTransaction? tx,
        DbConnectionType dbType,
        IDbSpeciffic? dbSpeciffic,
        DbNamingConvention dbNamingConvention = DbNamingConvention.SnakeCase)
    {
        _conn = conn;
        _tx = tx;
        _dbType = dbType;
        _dbSpeciffic = dbSpeciffic;
        _dbNamingConvention = dbNamingConvention;
    }

    public DbConnection Connection
    {
        get
        {
            if (_conn == null)
                throw new NullReferenceException(nameof(Connection));

            return _conn;
        }
    }

    public DbTransaction? Transaction { get { return _tx; } }
    public DbConnectionType DbType { get { return _dbType; } }
    
    public IDbSpeciffic DatabaseSpeciffic
    {
        get
        {
            if (_dbSpeciffic == null)
                throw new NullReferenceException(nameof(DatabaseSpeciffic));

            return _dbSpeciffic;
        }
    }

    public DbNamingConvention NamingConvention 
    {
        get
        {
            return _dbNamingConvention;
        }
    }


    public async Task BeginTransactionAsync()
    {
        if (_conn == null)
            throw new NullReferenceException(nameof(Connection));

        _tx = await _conn.BeginTransactionAsync();
    }

    public async Task CommitAsync()
    {
        if (_tx == null)
            throw new NullReferenceException(nameof(Transaction));

        await _tx.CommitAsync();

        _tx = null;
    }

    public async Task RollbackAsync()
    {
        if (_tx == null)
            throw new NullReferenceException(nameof(Transaction));

        await _tx.RollbackAsync();

        _tx = null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_tx != null)
            await RollbackAsync();

        await CloseAsync();
    }

    async ValueTask IAsyncDisposable.DisposeAsync()
    {
        if (_tx != null)
            await RollbackAsync();

        await CloseAsync();
    }

    public async Task CloseAsync()
    {
        if (_tx != null)
            await RollbackAsync();

        if (_conn != null && _conn.State != ConnectionState.Closed)
            await _conn.CloseAsync();
    }
}
