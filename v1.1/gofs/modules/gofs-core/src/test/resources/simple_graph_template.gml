graph [
  directed 1
  vertex_properties [
    prop1 [
      is_static 1
      type "string"
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
      type "string"
    ]
  ]
  node [
    id 1
    prop1 "static value1"
    prop2 "dynamic default2"
  ]
  node [
    id 2
    prop1 "static value1"
  ]
  node [
    id 3
    prop1 "static value1"
  ]
  node [
    id 4
    prop1 "static value1"
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
  edge [
    id 3
    source 3
    target 4
  ]
]