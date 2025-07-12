def reassign_leader_on_disconnect(old_leader_id: str):
    """
    Khi node leader disconnect, chuyển quyền leader cho follower đầu tiên,
    cập nhật lại bảng block-manager và active_node_manager.
    """
    print(f"[DEBUG] === Starting reassign_leader_on_disconnect ===")
    print(f"[DEBUG] Old leader ID: {old_leader_id}")
    
    conn_meta = None
    try:
        print(f"[DEBUG] Connecting to metadata database...")
        conn_meta = psycopg2.connect(**DB)
        print(f"[DEBUG] Successfully connected to metadata database")
        
        with conn_meta.cursor() as cur:
            # 1. Lấy block node này đang làm leader (task)
            print(f"[DEBUG] Step 1: Getting current task for node {old_leader_id}")
            cur.execute("""
                SELECT task FROM active_node_manager
                 WHERE node_id = %s
            """, (old_leader_id,))
            row = cur.fetchone()
            print(f"[DEBUG] Query result for task: {row}")
            
            if not row or not row[0] or row[0] == 'free':
                print(f"[DEBUG] Node {old_leader_id} không có task để reassign. Task value: {row[0] if row else 'None'}")
                return
            
            block_id = row[0]
            print(f"[DEBUG] Found block_id to reassign: {block_id}")

            # 2. Xác định table và file_base
            print(f"[DEBUG] Step 2: Determining table name from block_id")
            table = get_table_name_from_block_id(block_id)
            file_base = block_id.rsplit('.csv',1)[0].rsplit('_block',1)[0]
            print(f"[DEBUG] Table name: {table}")
            print(f"[DEBUG] File base: {file_base}")

            # 3. Lấy followers hiện tại từ file-specific database
            print(f"[DEBUG] Step 3: Getting current followers for block {block_id}")
            conn_file = None
            try:
                print(f"[DEBUG] Connecting to file database: {file_base}")
                conn_file = get_file_conn(file_base)
                print(f"[DEBUG] Successfully connected to file database")
                
                with conn_file.cursor() as cur_file:
                    cur_file.execute(
                        sql.SQL('SELECT followers FROM {} WHERE block_id = %s').format(
                            sql.Identifier(table)),
                        (block_id,))
                    result = cur_file.fetchone()
                    print(f"[DEBUG] Followers query result: {result}")
                    
                    if not result or not result[0]:
                        print(f"[DEBUG] Block {block_id} không có followers. Result: {result}")
                        return
                    
                    followers = result[0]
                    print(f"[DEBUG] Current followers list: {followers}")
                    print(f"[DEBUG] Number of followers: {len(followers) if followers else 0}")
                    
                    if not followers:
                        print(f"[DEBUG] Block {block_id} không có followers (empty list).")
                        return

                    # 4. Chọn follower đầu tiên làm leader mới
                    print(f"[DEBUG] Step 4: Selecting new leader from followers")
                    new_leader = followers[0]
                    new_followers = followers[1:] if len(followers) > 1 else []
                    print(f"[DEBUG] New leader selected: {new_leader}")
                    print(f"[DEBUG] Remaining followers: {new_followers}")

                    # 5. Update block-manager table in file database
                    print(f"[DEBUG] Step 5: Updating block-manager table {table}")
                    cur_file.execute(
                        sql.SQL("""
                            UPDATE {} SET leader = %s, followers = %s, status = 'pending'
                             WHERE block_id = %s
                        """).format(sql.Identifier(table)),
                        (new_leader, new_followers, block_id)
                    )
                    affected_rows = cur_file.rowcount
                    print(f"[DEBUG] Updated {affected_rows} rows in block-manager table")
                
                # Commit file database changes
                conn_file.commit()
                print(f"[DEBUG] Committed file database changes")
                
            except Exception as e:
                print(f"[DEBUG ERROR] Failed to query/update file database {file_base}: {e}")
                if conn_file:
                    conn_file.rollback()
                return
            finally:
                if conn_file:
                    conn_file.close()
                    print(f"[DEBUG] Closed file database connection")

            # 6. Update active_node_manager cho leader mới (in metadata database)
            print(f"[DEBUG] Step 6: Updating active_node_manager for new leader")
            try:
                # First check if new leader exists in active_node_manager
                cur.execute(
                    "SELECT node_id, task, status FROM active_node_manager WHERE node_id = %s",
                    (new_leader,)
                )
                leader_status = cur.fetchone()
                print(f"[DEBUG] New leader current status: {leader_status}")
                
                cur.execute(
                    "UPDATE active_node_manager SET task = %s WHERE node_id = %s",
                    (block_id, new_leader)
                )
                affected_rows = cur.rowcount
                print(f"[DEBUG] Updated {affected_rows} rows for new leader in active_node_manager")
                
                if affected_rows == 0:
                    print(f"[DEBUG WARNING] No rows updated for new leader {new_leader} - node may not exist in active_node_manager")
                
            except Exception as e:
                print(f"[DEBUG ERROR] Failed to update active_node_manager for new leader: {e}")
                raise

            # 7. Remove task từ node cũ (vì đã disconnect)
            print(f"[DEBUG] Step 7: Freeing old leader task")
            try:
                cur.execute(
                    "UPDATE active_node_manager SET task = 'free' WHERE node_id = %s",
                    (old_leader_id,)
                )
                affected_rows = cur.rowcount
                print(f"[DEBUG] Freed task for {affected_rows} rows (old leader)")
            except Exception as e:
                print(f"[DEBUG ERROR] Failed to free old leader task: {e}")
                raise

            print(f"[DEBUG] Successfully reassigned leader of {block_id} from {old_leader_id} to {new_leader}")

        # 8. Commit metadata changes
        print(f"[DEBUG] Step 8: Committing metadata changes")
        conn_meta.commit()
        print(f"[DEBUG] All metadata changes committed successfully")
        
        # 9. Send notification to new leader (optional)
        print(f"[DEBUG] Step 9: Sending leader promotion notification to {new_leader}")
        try:
            send_to_datanode(new_leader, {
                'type': 'promote_to_leader',
                'block_id': block_id,
                'file': file_base,
                'old_leader': old_leader_id
            })
            print(f"[DEBUG] Successfully sent promotion notification to {new_leader}")
        except Exception as e:
            print(f"[DEBUG WARNING] Failed to send promotion notification: {e}")
        
    except Exception as e:
        print(f"[DEBUG ERROR] Exception in reassign_leader_on_disconnect: {e}")
        if conn_meta:
            print(f"[DEBUG] Rolling back metadata transaction due to error")
            conn_meta.rollback()
        raise
    finally:
        if conn_meta:
            conn_meta.close()
            print(f"[DEBUG] Closed metadata database connection")
        print(f"[DEBUG] === End reassign_leader_on_disconnect ===")