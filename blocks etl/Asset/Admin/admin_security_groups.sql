with tables as (
    /* objects that users have explicitly been granted access to */
    select
        uopdb as database_objid,
        uopobject as object_objid,
        uopobjpriv as privileges,
        uopgobjpriv as g_privileges,
        uopuser as user_objid,
        null::int4 as group_objid
    from {{source('cdw', '_t_usrobj_priv')}}
    where uopobject != 0 /* skip admin privileges */
    union all
    /* get users that dont have permissions directly granted. */
    select
        0,
        0,
        0,
        0,
        objid,
        0 /* permissions may be inherited through groups. */
    from {{source('cdw', '_v_user')}}
    where
        objid != 4900 /* exclude admin user */
        and objid not in (
            select distinct uopuser
            from {{source('cdw', '_t_usrobj_priv')}}
        )
    union all
    /* objects that groups have explicitly been granted access to */
    select
        gopdb as database_objid,
        gopobject as object_objid,
        gopobjpriv as privileges,
        gopgobjpriv as g_privileges,
        null::int4 as user_objid,
        gopgroup as group_objid
    from {{source('cdw', '_t_grpobj_priv')}}
    where gopobject != 0 /* skip admin privileges */
),
details as (
    select distinct
        case when _v_user.username is not null then 'user'
            when _v_group.groupname is not null then 'group'
            else 'unknown'
        end as security_level,
        coalesce(_v_user.username, _v_group.groupname) as group_name,
        coalesce(_v_database.database, 'global' ) as database_name,
        _t_object.objname as relation_name,
        coalesce(_v_relobjclasses.classname, 'class' ) as relation_type,
        coalesce(blocks_columns.column_name, prod_columns.column_name) as column_name
    from
        tables
        inner join {{source('cdw', '_t_object')}} as _t_object
            on tables.object_objid = _t_object.objid
        left join {{source('cdw', '_v_relobjclasses')}} as _v_relobjclasses
            on _t_object.objclass = _v_relobjclasses.objclass
        left join {{source('cdw', '_v_database')}} as _v_database
            on tables.database_objid = _v_database.objid
        left join {{source('cdw', '_v_user')}} as _v_user
            on tables.user_objid = _v_user.objid
        left join {{source('cdw', '_v_group')}} as _v_group
            on tables.group_objid = _v_group.objid
        left join {{source('manual', '_v_sys_columns')}} as blocks_columns
            on _t_object.objname = blocks_columns.table_name
        left join {{source('cdw', '_v_sys_columns')}} as prod_columns
            on _t_object.objname = prod_columns.table_name
)
select 
    {{
        dbt_utils.surrogate_key([
            'security_level',
            'group_name',
            'database_name',
            'relation_name',
            'relation_type',
            'column_name'
        ])
    }} as admin_security_group_key,
    details.*
from details
