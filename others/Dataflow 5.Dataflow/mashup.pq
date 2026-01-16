[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Sales_new_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Sales_new = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "9ec5a999-67d7-41a8-8979-6c8ebda21878"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "d9ebad35-6987-4924-8dbf-677847936bf0"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Sales_new", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared Sales_new_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "9ec5a999-67d7-41a8-8979-6c8ebda21878"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "29ddef12-09d4-4dc3-a7b9-b4822e7df5ad"]}[Data],
  TableNavigation = Navigation_2{[Id = "Sales_new_test", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
