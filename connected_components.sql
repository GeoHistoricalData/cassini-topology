CREATE OR REPLACE FUNCTION iterative_depth_first_search(atopology varchar, anode varchar, anedge varchar, acomponent varchar, initial int, component_id int)
RETURNS VOID AS $$
DECLARE
  v integer;
  w integer;
  comp integer;
  empty boolean;
BEGIN
  -- let S be a stack
  EXECUTE 'CREATE TEMP TABLE stack (id integer);';
  -- S.push(v)
  EXECUTE format('INSERT INTO stack VALUES ($1)') USING initial;
  -- while S is not empty
  LOOP
    -- v ← S.pop() 
    SELECT id INTO v FROM stack LIMIT 1;
    EXECUTE format('DELETE FROM stack WHERE id = $1') USING v;
    --RAISE NOTICE 'v = %', v;
    -- if v is not labeled as discovered:
    -- SELECT component INTO comp FROM export.node WHERE node_id = v;
    EXECUTE format('SELECT %I FROM %I.%I WHERE node_id = $1', acomponent, atopology, anode) INTO comp USING v;
    IF comp IS NULL THEN
      -- label v as discovered
      EXECUTE format('UPDATE %I.%I SET %I = $1 WHERE node_id = $2', atopology, anode, acomponent) USING component_id, v;
      -- for all edges from v to w in G.adjacentEdges(v) do S.push(w)
      --INSERT INTO stack SELECT distinct(new_id) FROM 
      --  (SELECT start_node AS new_id FROM export.edge WHERE end_node = v UNION SELECT end_node AS new_id FROM export.edge WHERE start_node = v) AS tmp
      --  WHERE NOT EXISTS (SELECT 1 FROM stack WHERE id = new_id);
      EXECUTE format('INSERT INTO stack SELECT distinct(new_id) FROM '
        || '(SELECT start_node AS new_id FROM %I.%I WHERE end_node = $1 UNION SELECT end_node AS new_id FROM %I.%I WHERE start_node = $1) AS tmp '
        || 'WHERE NOT EXISTS (SELECT 1 FROM stack WHERE id = new_id)',
        atopology, anedge, atopology, anedge) USING v;
      SELECT count(*) INTO v FROM stack;
      --RAISE NOTICE '   stack = %', v;
    END IF;
    SELECT NOT EXISTS INTO empty (SELECT 1 FROM stack);
    --RAISE NOTICE '   empty = %', empty;
    EXIT WHEN empty;
  END LOOP;
  EXECUTE 'DROP TABLE stack';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION connected_components(atopology varchar, anode varchar, anedge varchar, acomponent varchar)
RETURNS VOID AS $$
DECLARE
  id integer;
  comp integer;
BEGIN
  comp := 1;
  LOOP
    --SELECT node_id INTO id FROM export.node WHERE component IS NULL LIMIT 1;
    EXECUTE format('SELECT node_id FROM %I.%I WHERE %I IS NULL LIMIT 1', atopology, anode, acomponent) INTO id;
    IF id IS NULL THEN
      EXIT;
    END IF;
    RAISE NOTICE 'connected_components %', comp;
    EXECUTE format('SELECT iterative_depth_first_search(%L, %L, %L, %L, $1, $2)', atopology, anode, anedge, acomponent) USING id, comp;
    comp := comp + 1;
  END LOOP;
END;
$$ LANGUAGE plpgsql;
