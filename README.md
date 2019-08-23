# The Contagion of Cheating

## Problems
*	How to establish strength of effect, not just whether it is present
*	Why we need motif analysis
    *	Observations not independent
    *	Killing network has peculiar structure
        *	Indegree distribution peaked at 1
        *	Temporal ordering: outlinks always come before the inlink

## Tasks

###	Optimizing code

*	Organize code in functions to call repeatedly in loops and with different parameters
*	Save data from intermediary steps to reuse multiple times
*	Save mapping of shuffled networks to later join with edges/nodes data 
*	Save mapping of who is visible to whom to later join with edges/nodes data 

###   Randomizing networks

*	Randomize within teams, within cheaters and non-cheaters

###	Level of harm

*	Test for different levels of harm (top 90, 80, 70, 60, 50, 40, 30, 20, 10%)
*	Get descriptives
    * Indegree distribution – overall + cheater indegree
* Test for multiple killings from cheaters 
    * 3-actor motifs (killed by 2 cheaters)
    * (4-actor motifs (killed by 3 cheaters))
    * (Test for different levels of harm)
    
###	Observing cheaters

*	Create observation network (links go from the killer to the observer)
    * No need to shuffle networks separately, create observation network from each saved shuffled network
*	Get descriptives
    * Indegree distribution – overall + cheater-only indegree
*	Test for observing multiple killings vs. observing killings by multiple cheaters
    * 2-actor motif with 2/3/4/5 links (2/3/4/5 killings from 1 cheater)
    * 3-actor motif with 2/3/4/5 links each (2/3/4/5 killings each by 2 different cheaters)
    * 4-actor motif with 2/3/4/5 links each (2/3/4/5 killings each by 3 different cheaters)


###	Observing and experiencing harm

* 2-actor motif with 1 killed-link and 2 observed-links
* 3-actor motif with 1 killed-link and 2 observed-links (killed by one cheater, observed another)



