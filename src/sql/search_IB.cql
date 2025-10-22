MATCH (start:Airport {name:'$dep_airport'}), (end:Airport {name:'$arr_airport'})
CALL apoc.path.expandConfig(start, {
  relationshipFilter: "flights>",
  minLevel: 1,
  maxLevel: 5,
  bfs: false,
  uniqueness: "NODE_PATH",
  filterStartNode: true,
  terminatorNodes: [end],    // ✅ 用节点变量，不是字符串
  endNodes: [end],
  filterFunction: "
    prevRel IS NULL OR (
      datetime(r.take_off_time) >= datetime(prevRel.take_off_time) + duration({minutes: toInteger(prevRel.flight_time) + 60}) AND
      datetime(r.take_off_time) <= datetime(prevRel.take_off_time) + duration({minutes: toInteger(prevRel.flight_time) + 720})
    )
  "
})
YIELD path
RETURN [a IN nodes(path) | a.name] AS airport_chain,
       [r IN relationships(path) | {
         from: startNode(r).name,
         to: endNode(r).name,
         take_off_time: r.take_off_time,
         flight_time: r.flight_time
       }] AS flights;