# The Evidence

An iPhone-installable reader of new research findings from leading universities, filtered to everyday
health, performance, money and wellbeing. Same static site + hourly GitHub Action pattern as The
Commonplace, Degrees, Jus Cogens and Current.

**Sourcing rule.** Only the institutions listed in `SOURCES` in `scripts/update.py` count. Each story comes
from the university's own newsroom feed, or from Google News restricted to that university's domains; every
item's address (or Google News publisher address) is checked against the institution's domains, and anything
else is rejected. Edit the `uni(...)` lines to change the list.

**Domains.** Longevity (aging, heart, metabolic, cancer), Brain & mood, Sleep, Fitness, Nutrition & gut
(incl. supplements), Hormones, Skin, Environment, Money & AI, Social. A finding must match a domain in its
headline (or twice in its summary) and be about people's lives to be kept.

**Evidence labels.** Review of studies, Clinical trial, Human study, Animal study, Lab study, inferred from the
text. Animal and lab findings are early evidence, not advice.

This is a reading list, not medical advice.
