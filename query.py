def prepare_sum_sql(table_name, entity, resource, epoch):
    return """
    SELECT {entity}, SUM({resource}) AS resource
        FROM {table_name}
        WHERE {epoch} = {{epoch_no}}
        GROUP BY {entity}
        ORDER BY resource DESC;
    """.format(table_name=table_name, entity=entity, resource=resource, epoch=epoch)


def prepare_cnt_sql(table_name, entity):
    return """
    select {entity}, count(*) as resource 
        from {table_name} 
        where epoch_no= {{epoch_no}}
        group by {entity} 
        order by resource desc;
    """.format(table_name=table_name, entity=entity)


def prepare_pool_block_sql():
    return """
        select sl.pool_hash_id as pool_id, count(*) as resource
            from slot_leader as sl, block
            where block.slot_leader_id = sl.id
                and block.epoch_no = {epoch_no}
            group by block.slot_leader_id, sl.pool_hash_id
            order by resource desc;
    """


def prepare_epoch_reward_sql(epoch):
    return """
        select addr_id, sum(amount) from reward where earned_epoch = {epoch} group by addr_id
    """.format(epoch=epoch)


def prepare_pool_offline_data():
    return """
        SELECT pool_id, hompage  as resource  FROM (
            SELECT pool_id, row_number() over 
            (partition by pool_id order by id desc), json->'homepage' as hompage
            FROM pool_offline_data
        ) AS de_dump WHERE row_number = 1;
    """


def prepare_utxo_sql(timestp):
    return '''
      select tx_out.address as address, tx_out.value as lovelace, generating_block.time as timestamp
    from (select to_timestamp ({timestp}, 'YYYY-MM-DD HH24:MI:SS') as effective_time_) as const
    cross join tx_out
    inner join tx as generating_tx on generating_tx.id = tx_out.tx_id
    inner join block as generating_block on generating_block.id = generating_tx.block_id
    left join tx_in as consuming_input on consuming_input.tx_out_id = generating_tx.id
      and consuming_input.tx_out_index = tx_out.index
    left join tx as consuming_tx on consuming_tx.id = consuming_input.tx_in_id
    left join block as consuming_block on consuming_block.id = consuming_tx.block_id
    where ( -- Ommit outputs from genesis after Allegra hard fork
			const.effective_time_ < '2020-12-16 21:44:00'
			or generating_block.epoch_no is not null
			)
      and const.effective_time_ >= generating_block.time -- Only outputs from blocks generated in the past
      and ( -- Only outputs consumed in the future or unconsumed outputs
		const.effective_time_ <= consuming_block.time or consuming_input.id IS NULL
		) ;
    '''.format(timestp=timestp)


def prepare_utxo_epoch(epoch):
    return """
    select tx_out.address as address, tx_out.value as lovelace from tx_out
    inner join tx as generating_tx on generating_tx.id = tx_out.tx_id
    inner join block as generating_block on generating_block.id = generating_tx.block_id
    left join tx_in as consuming_input on consuming_input.tx_out_id = generating_tx.id
      and consuming_input.tx_out_index = tx_out.index
    left join tx as consuming_tx on consuming_tx.id = consuming_input.tx_in_id
    left join block as consuming_block on consuming_block.id = consuming_tx.block_id
    where ( -- Omit outputs from genesis after Allegra hard fork
-- 			const.effective_time_ < '2020-12-16 21:44:00' or
			generating_block.epoch_no is not null
			)
--       and const.effective_time_ >= generating_block.time -- Only outputs from blocks generated in the past
      and generating_block.epoch_no <= {epoch}
      and ( -- Only outputs consumed in the future or unconsumed outputs
-- 		const.effective_time_ <= consuming_block.time or consuming_input.id IS NULL
		{epoch} < consuming_block.epoch_no or consuming_input.id IS NULL
		);
    """.format(epoch=epoch)


def prepare_genesis_sql():
    return """
    select genesis_output.address as address, genesis_output.value as lovelace
    from block as genesis_block
    inner join tx as genesis_tx on genesis_tx.block_id = genesis_block.id
    inner join tx_out as genesis_output on genesis_output.tx_id = genesis_tx.id
    where genesis_block.epoch_no is null;
    """
