from django.db import migrations, models


TABLE_NAME = 'nbxsync_zabbixhostinterface'
LEGACY_CONSTRAINT_NAMES = (
    'nbxsync_zabbixhostinterface_unique__server_type_object',
    'nbxsync_zabbixhostinterface_unique_server_type_object',
)
CANONICAL_LEGACY_CONSTRAINT_NAME = LEGACY_CONSTRAINT_NAMES[0]


def drop_legacy_constraints(apps, schema_editor):
    table = schema_editor.quote_name(TABLE_NAME)
    for constraint_name in LEGACY_CONSTRAINT_NAMES:
        constraint = schema_editor.quote_name(constraint_name)
        schema_editor.execute(f'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}')


def restore_legacy_constraint(apps, schema_editor):
    table = schema_editor.quote_name(TABLE_NAME)
    constraint = schema_editor.quote_name(CANONICAL_LEGACY_CONSTRAINT_NAME)
    schema_editor.execute(
        f'ALTER TABLE {table} ADD CONSTRAINT {constraint} '
        'UNIQUE (zabbixserver_id, type, assigned_object_type_id, assigned_object_id)'
    )


class Migration(migrations.Migration):

    dependencies = [
        ('nbxsync', '0005_alter_zabbixhostgroupassignment_assigned_object_type_and_more'),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunPython(
                    drop_legacy_constraints,
                    reverse_code=restore_legacy_constraint,
                ),
            ],
            state_operations=[
                migrations.RemoveConstraint(
                    model_name='zabbixhostinterface',
                    name='nbxsync_zabbixhostinterface_unique__server_type_object',
                ),
            ],
        ),
        migrations.AddConstraint(
            model_name='zabbixhostinterface',
            constraint=models.UniqueConstraint(
                fields=('zabbixserver', 'type', 'assigned_object_type', 'assigned_object_id'),
                condition=models.Q(('interface_type', 1)),
                name='nbxsync_zabbixhostinterface_unique__default_server_type_object',
                violation_error_message='A default Hostinterface with this type has already been defined',
            ),
        ),
    ]
