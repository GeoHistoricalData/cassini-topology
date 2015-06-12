SELECT create_road_topology_cassini(); 

-- drop component column
--ALTER TABLE france_cassini_routes_topo.cassini_node DROP COLUMN component;
-- add new component column
ALTER TABLE france_cassini_routes_topo.cassini_node ADD COLUMN component integer DEFAULT NULL;

-- compute the connected components
SELECT connected_components('france_cassini_routes_topo', 'cassini_node', 'cassini_edge', 'component');

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

-- compute the geometry of the connected components (union of its components geometries)
SELECT AddGeometryColumn ('france_cassini_routes_topo','connected_component','geom',2154,'GEOMETRY',2);
UPDATE france_cassini_routes_topo.connected_component c SET geom = (SELECT ST_Union(geom) FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id);

-- add indices
CREATE INDEX connected_component_index ON france_cassini_routes_topo.connected_component USING gist(geom);
CREATE INDEX connected_component_index_pk ON france_cassini_routes_topo.connected_component USING btree(component_id);
GRANT SELECT ON TABLE france_cassini_routes_topo.connected_component TO ghdb_user;

-- add new component column
ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN red double precision DEFAULT 0;
ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN white double precision DEFAULT 0;
ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN trail double precision DEFAULT 0;
ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN forest double precision DEFAULT 0;
ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN ferry double precision DEFAULT 0;
ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN gap double precision DEFAULT 0;
ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN bridge double precision DEFAULT 0;
ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN fictive double precision DEFAULT 0;

UPDATE france_cassini_routes_topo.connected_component c SET
	red = coalesce((SELECT sum(ST_Length(geom))/c.length FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id AND e.road_type='red'), 0),
	white = coalesce((SELECT sum(ST_Length(geom))/c.length FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id AND e.road_type='white'), 0),
	trail = coalesce((SELECT sum(ST_Length(geom))/c.length FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id AND e.road_type='trail'), 0),
	forest = coalesce((SELECT sum(ST_Length(geom))/c.length FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id AND e.road_type='forest'), 0),
	ferry = coalesce((SELECT sum(ST_Length(geom))/c.length FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id AND e.road_type='ferry'), 0),
	gap = coalesce((SELECT sum(ST_Length(geom))/c.length FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id AND e.road_type='gap'), 0),
	bridge = coalesce((SELECT sum(ST_Length(geom))/c.length FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id AND e.road_type='bridge'), 0),
	fictive = coalesce((SELECT sum(ST_Length(geom))/c.length FROM france_cassini_routes_topo.cassini_edge e WHERE e.component = c.component_id AND e.road_type='fictive'), 0);

ALTER TABLE france_cassini_routes_topo.connected_component ADD COLUMN component_type varchar DEFAULT 'mixed';
UPDATE france_cassini_routes_topo.connected_component c SET component_type = 'red' WHERE red > 0.75;
UPDATE france_cassini_routes_topo.connected_component c SET component_type = 'white' WHERE white > 0.75;
UPDATE france_cassini_routes_topo.connected_component c SET component_type = 'trail' WHERE trail > 0.75;
UPDATE france_cassini_routes_topo.connected_component c SET component_type = 'forest' WHERE forest > 0.75;
UPDATE france_cassini_routes_topo.connected_component c SET component_type = 'ferry' WHERE ferry > 0.75;
UPDATE france_cassini_routes_topo.connected_component c SET component_type = 'gap' WHERE gap > 0.75;
UPDATE france_cassini_routes_topo.connected_component c SET component_type = 'bridge' WHERE bridge > 0.75;
UPDATE france_cassini_routes_topo.connected_component c SET component_type = 'fictive' WHERE fictive > 0.75;

--DROP TABLE france_cassini_routes_topo.connected_component_distance;
CREATE TABLE france_cassini_routes_topo.connected_component_distance AS
  SELECT ST_Distance(n1.geom, n2.geom) AS distance, n1.component_id AS component_id1, n2.component_id AS component_id2, ST_ShortestLine(n1.geom, n2.geom) AS geom
	FROM france_cassini_routes_topo.connected_component n1
		LEFT JOIN france_cassini_routes_topo.connected_component n2
		ON ST_DWithin(n1.geom, n2.geom, 200)
	WHERE n1.component_id < n2.component_id AND n1.component_id <> n2.component_id;
