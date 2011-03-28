
Changelog
=========

Tracks any changes to the EPU Controller and/or Decision Engine that warrant a change to the Decision Engine Implementer's Guide.

2010-05-25 -- Initial version
2010-10-27 -- reconfigure() allows an outside entity to alter the configuration of the engine.  How a DE responds to
              this is DE-specific (it may even be unimplemented).
2011-03-18 -- de_state() allows a DE to assert a "stability" statement, accessible via controller op.  How a DE
              responds to this is DE-specific (it may even be unimplemented).
