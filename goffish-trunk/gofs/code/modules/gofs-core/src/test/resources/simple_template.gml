graph [
  directed 1
  vertex_properties [
    prop1 [
      is_static 1
      type "list"
    ]
    prop2 [
      is_static 0
      type "string"
    ]
  ]
  edge_properties [
    prop1 [
      is_static 0
      type "string"
    ]
    prop2 [
      is_static 0
      type "list"
    ]
  ]
  node [
    id 1
    prop1 [ value "1" value "2" ]
    prop2 "dynamic default2"
  ]
  node [
    id 2
    prop1 "static value1"
  ]
  node [
    id 3
    remote 2
  ]
  edge [
    id 1
    source 1
    target 2
    prop1 "dynamic default1"
  ]
  edge [
    id 2
    source 2
    target 3
  ]
]