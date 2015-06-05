-- drop component column
--ALTER TABLE france_cassini_routes_topo.cassini_node DROP COLUMN component;
-- add new component column
ALTER TABLE france_cassini_routes_topo.cassini_node ADD COLUMN component integer DEFAULT NULL;

-- compute the connected components
SELECT connected_components('france_cassini_routes_topo', 'cassini_node', 'cassini_edge');

-- drop component column
--ALTER TABLE france_cassini_routes_topo.cassini_edge DROP COLUMN component;
-- add component column
ALTER TABLE france_cassini_routes_topo.cassini_edge ADD COLUMN component integer DEFAULT NULL;
-- set the component from the nodes
UPDATE france_cassini_routes_topo.cassini_edge e SET component = (SELECT component FROM france_cassini_routes_topo.cassini_node n WHERE n.node_id = e.start_node);

-- create a components table
CREATE TABLE france_cassini_routes_topo.connected_component AS
  SELECT distinct(component) AS component_id,
  count(*) AS nb_edge,
  sum(ST_Length(geom)) AS length
  FROM france_cassini_routes_topo.cassini_edge
  GROUP BY component;

SELECT AddGeometryColumn ('france_cassini_routes_topo','connected_component','geom',2154,'MULTILINESTRING',2);
UPDATE france_cassini_routes_topo.connected_component c SET geom = (SELECT ST_Union(geom) FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id);

CREATE INDEX connected_component_index ON france_cassini_routes_topo.connected_component USING gist(geom);
CREATE INDEX connected_component_index_pk ON france_cassini_routes_topo.connected_component USING btree(component_id);
