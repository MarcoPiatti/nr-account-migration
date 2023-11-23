import os
import argparse
import library.utils as utils
import library.migrationlogger as m_logger
import library.localstore as store
import library.clients.entityclient as ec
import library.status.dashboard_status as ds

log = m_logger.get_logger(os.path.basename(__file__))


def print_args(args, src_api_key, src_region, tgt_api_key, tgt_region):
    log.info("Using fromFile : " + args.fromFile[0])
    log.info("Using sourceAccount : " + str(args.sourceAccount[0]))
    log.info("Using sourceApiKey : " + len(src_api_key[:-4]) * "*" + src_api_key[-4:])
    log.info("sourceRegion : " + src_region)
    log.info("Using targetAccount : " + str(args.targetAccount[0]))
    log.info("Using targetApiKey : " + len(tgt_api_key[:-4]) * "*" + tgt_api_key[-4:])
    log.info("targetRegion : " + tgt_region)


def configure_parser():
    parser = argparse.ArgumentParser(description='Migrate Dashboards')
    parser.add_argument('--fromFile', nargs=1, type=str, required=True,
                        help='Path to file with dashboard names(newline separated)')
    parser.add_argument('--sourceAccount', nargs=1, type=int, required=True, help='Source accountId')
    parser.add_argument('--sourceApiKey', nargs=1, type=str, required=True, help='Source account API Key or \
                                                                        set environment variable ENV_SOURCE_API_KEY')
    parser.add_argument('--sourceRegion', type=str, nargs=1, required=False, help='sourceRegion us(default) or eu')
    parser.add_argument('--targetAccount', nargs=1, type=int, required=True, help='Target accountId')
    parser.add_argument('--targetApiKey', nargs=1, type=str, required=True, help='Target API Key, \
                                                                    or set environment variable ENV_TARGET_API_KEY')
    parser.add_argument('--targetRegion', type=str, nargs=1, required=False, help='targetRegion us(default) or eu')
    return parser


def get_dashboard(per_api_key, name, all_db_status, acct_id, *, get_widgets=False, region='us'):
    result = ec.get_dashboard_definition(per_api_key, name, acct_id, region)
    if not result:
        all_db_status[name][ds.DASHBOARD_FOUND] = False
        return None
    all_db_status[name][ds.DASHBOARD_FOUND] = True
    if not get_widgets:
        return result
    widgets_result = ec.get_dashboard_widgets(per_api_key, result['guid'], region)
    if 'error' in widgets_result:
        all_db_status[name][ds.ERROR] = result['error']
        log.error('Error fetching dashboard widgets' + name + '  ' + result['error'])
        return None
    if not widgets_result['entityFound']:
        all_db_status[name][ds.WIDGETS_FOUND] = False
        return None
    all_db_status[name][ds.WIDGETS_FOUND] = True
    return widgets_result['entity']


def update_nrql_account_ids(src_acct_id, tgt_acct_id, entity):
    if not 'pages' in entity:
        return
    for page in entity['pages']:
        if not 'widgets' in page:
            continue
        for widget in page['widgets']:
            if not 'rawConfiguration' in widget:
                continue
            if not 'nrqlQueries' in widget['rawConfiguration']:
                continue
            for query in widget['rawConfiguration']['nrqlQueries']:
                if 'accountId' in query and query['accountId'] == src_acct_id:
                    query['accountId'] = tgt_acct_id
                if 'accountIds' in query and query['accountIds'][0] == src_acct_id:
                    query['accountIds'][0] = tgt_acct_id


def get_entity_name_and_type_by_id(api_key, entity_id):
    for entity_type in [ec.APM_APP, ec.APM_KT, ec.BROWSER_APP, ec.MOBILE_APP]:
        result = ec.get_entity(api_key, entity_type, entity_id)
        if not result['entityFound']:
            continue
        return {'entity_name': result['entity']['name'], 'entity_type': entity_type}
    return None


def get_entity_id_by_name_and_type(acct_id, api_key, entity_name, entity_type):
    result = ec.get_entity_by_name(api_key, acct_id, entity_type, entity_name)
    if not result['entityFound']:
        return None
    if entity_type == ec.APM_APP:
        return result['entity']['applicationId']
    return result['entity']['id']


def update_basic_query_ids(src_acct_id, src_api_key, tgt_acct_id, tgt_api_key, dashboard, basic_query_entity_ids):
    if 'pages' not in dashboard:
        return
    for page in dashboard['pages']:
        if 'widgets' not in page:
            continue
        for widget in page['widgets']:
            if 'rawConfiguration' not in widget:
                continue
            if 'accountId' in widget['rawConfiguration'] and widget['rawConfiguration']['accountId'] == src_acct_id:
                widget['rawConfiguration']['accountId'] = tgt_acct_id
            if 'entityIds' not in widget['rawConfiguration']:
                continue
            for i in range(len(widget['rawConfiguration']['entityIds'])):
                entity_id = widget['rawConfiguration']['entityIds'][i]
                if entity_id not in basic_query_entity_ids:
                    src_entity_id = entity_id
                    result = get_entity_name_and_type_by_id(src_api_key, src_entity_id)
                    if result is None:
                        log.warn('Dashboard ' + dashboard['name'] + ': ' +
                                 'Could not determine required source entity with ID' + entity_id
                                 + ' for Basic Query Widget in dashboard')
                        continue
                    entity_name = result['entity_name']
                    entity_type = result['entity_type']
                    tgt_entity_id = get_entity_id_by_name_and_type(tgt_acct_id, tgt_api_key, entity_name, entity_type)
                    if tgt_entity_id is None:
                        log.warn('Dashboard ' + dashboard['name'] + ': ' +
                                 'Could not find required target entity ' + entity_name
                                 + ' with entity type ' + entity_type + '.'
                                 ' The entity probably has not been migrated yet')
                        continue
                    basic_query_entity_ids[src_entity_id] = tgt_entity_id
                widget['rawConfiguration']['entityIds'][i] = basic_query_entity_ids[entity_id]


def migrate_dashboards(from_file, src_acct, src_api_key, src_region, tgt_acct, tgt_api_key, tgt_region):
    log.info('Dashboard migration started.')
    db_names = store.load_names(from_file)
    all_db_status = {}
    basic_query_entity_ids = {}
    for db_name in db_names:
        all_db_status[db_name] = {}
        tgt_dashboard = get_dashboard(tgt_api_key, db_name, all_db_status, tgt_acct,
                                      get_widgets=False, region=tgt_region)
        if tgt_dashboard is not None:
            log.warning('Dashboard already exists in target skipping : ' + db_name)
            all_db_status[db_name][ds.TARGET_EXISTED] = True
            continue
        all_db_status[db_name][ds.TARGET_EXISTED] = False
        src_dashboard = get_dashboard(src_api_key, db_name, all_db_status, src_acct, get_widgets=True,
                                      region=src_region)
        if src_dashboard is None:
            continue
        log.info('Found source dashboard ' + db_name)
        tgt_dashboard = src_dashboard
        del tgt_dashboard['guid']
        update_nrql_account_ids(src_acct, tgt_acct, tgt_dashboard)
        update_basic_query_ids(src_acct, src_api_key, tgt_acct, tgt_api_key, tgt_dashboard, basic_query_entity_ids)
        result = ec.post_dashboard(tgt_api_key, tgt_dashboard, tgt_acct, tgt_region)
        all_db_status[db_name][ds.STATUS] = result['status']
        if result['entityCreated']:
            log.info('Created target dashboard ' + db_name)
            all_db_status[db_name][ds.DASHBOARD_CREATED] = True
            all_db_status[db_name][ds.TARGET_DASHBOARD] = result['entity']['guid']
    db_status_file = str(src_acct) + '_' + utils.file_name_from(from_file) + '_dashboards_' + str(tgt_acct) + '.csv'
    store.save_status_csv(db_status_file, all_db_status, ds)
    log.info('Dashboard migration complete.')


def main():
    parser = configure_parser()
    args = parser.parse_args()
    src_api_key = utils.ensure_source_api_key(args)
    if not src_api_key:
        utils.error_and_exit('source_api_key', 'ENV_SOURCE_API_KEY')
    tgt_api_key = utils.ensure_target_api_key(args)
    if not tgt_api_key:
        utils.error_and_exit('target_api_key', 'ENV_TARGET_API_KEY')
    src_region = utils.ensure_source_region(args)
    tgt_region = utils.ensure_target_region(args)
    print_args(args, src_api_key, src_region, tgt_api_key, tgt_region)
    migrate_dashboards(args.fromFile[0], args.sourceAccount[0], src_api_key, src_region, args.targetAccount[0],
                       tgt_api_key, tgt_region)


if __name__ == '__main__':
    main()
