using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Zen.DbAccess.Models;

public class SqlParam
{
    public SqlParam(string name)
    {
        this.name = name;
        this.value = DBNull.Value;
    }

    public SqlParam(string name, object? value)
    {
        this.name = name;
        this.value = value ?? DBNull.Value;
    }

    public SqlParam(SqlParam sqlParam)
    {
        this.name = sqlParam.name;
        this.value = sqlParam.value;
        this.size = sqlParam.size;
        this.isBlob = sqlParam.isBlob;
        this.isClob = sqlParam.isClob;
        this.paramDirection = sqlParam.paramDirection;
    }

    private ParameterDirection _paramDirection = ParameterDirection.Input;

    public ParameterDirection paramDirection
    {
        get
        {
            return _paramDirection;
        }

        set
        {
            _paramDirection = value;

            if (size <= 0
                && (_paramDirection == ParameterDirection.ReturnValue
                    || _paramDirection == ParameterDirection.InputOutput
                    || _paramDirection == ParameterDirection.Output))
            {
                size = 1024;
            }
        }
    }

    public bool isClob { get; set; } = false;
    public bool isBlob { get; set; } = false;
    public string name { get; private set; }
    public int size { get; set; }
    public object value { get; set; }
}
