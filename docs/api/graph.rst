Graph
=====

Graph exports retain payment and event identities while separating observable
evidence from oracle-only campaign truth. PyTorch Geometric conversion is an
optional dependency.

.. autosummary::
   :nosignatures:

   fraudtwin.GraphDataset
   fraudtwin.GraphNode
   fraudtwin.GraphEdge
   fraudtwin.GraphCampaign
   fraudtwin.GraphEvidence
   fraudtwin.GraphHyperedge
   fraudtwin.GraphHyperedgeMembership
   fraudtwin.build_graph
   fraudtwin.validate_graph
   fraudtwin.validate_graph_scenarios
   fraudtwin.write_graph
   fraudtwin.to_pyg
