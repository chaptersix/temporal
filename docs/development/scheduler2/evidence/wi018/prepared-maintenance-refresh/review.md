# Maintenance preview refresh follow-on

This two-file manifest supersedes the earlier prepared-maintenance owned source snapshot. Earlier evidence remains unchanged.

PublicListControl.published_target_count records requested output count independently of actual results. A completed empty exhausted window remains idle until target changes. With no pending job, readiness requests immediate maintenance when the target differs or the first published future instant expires, otherwise returns that instant as the next wake. Source-pending work suppresses preview evaluation while cleanup remains eligible.

Refresh reuses the current published static slot, preserving its limit and recent actions. It installs a new pending preview without changing the currently visible complete window. Boundary is max(execution now, disposed cursor, selected-profile fixed PublicInfo.UpdateTime). Pending jobs preserve their static owner/stamp on changed target or expiry of their own produced first output; expiry of the older published window does not repeatedly reset an unfinished replacement. Notes/recent-only changes do not reset a job. Target zero publishes without a source grant. Parent owns schema tag11 and merging the returned wake into physical coordinator scheduling.

Directed refresh02 PASS 0.679s (all seven maintenance tests). The first run failed only on comparing Go time.Time location representations; the corrected assertion compares instants. Scoped lint01 found no maintenance issue, but failed on shared coordinator demandsForSource complexity26 and public constructor unchecked assertion. Parent owns those fixes and final combined lint. No complete physical-task/fresh-runtime acceptance is claimed here.
