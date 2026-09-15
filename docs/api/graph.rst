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

.. autofunction:: fraudtwin.build_graph

.. autofunction:: fraudtwin.validate_graph

.. autofunction:: fraudtwin.validate_graph_scenarios

.. autofunction:: fraudtwin.write_graph

.. autofunction:: fraudtwin.to_pyg

.. autoclass:: fraudtwin.GraphDataset
   :members:
