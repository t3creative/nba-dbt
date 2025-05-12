Player Scoring Bxsc
    
    -- Game Stats - Calculated Metrics
        case 
            when cast(percentagePoints2pt as float) > 0 and cast(percentagePoints3pt as float) > 0 
            then least(cast(percentagePoints2pt as float), cast(percentagePoints3pt as float)) / 
                 greatest(cast(percentagePoints2pt as float), cast(percentagePoints3pt as float))
        end as scoring_balance_2pt_3pt,

        case 
            when cast(percentagePointsPaint as float) > 0 and cast(percentagePoints3pt as float) > 0 
            then cast(percentagePointsPaint as float) / cast(percentagePoints3pt as float)
        end as inside_outside_ratio,

        case 
            when cast(percentageUnassistedFGM as float) > 0 and cast(percentageAssistedFGM as float) > 0 
            then cast(percentageUnassistedFGM as float) / cast(percentageAssistedFGM as float)
        end as creation_ratio,

Player Tracking Bxsc

    -- Game Stats - Calculated Metrics
            case 
                when cast(distance as float) > 0 and cast({{ extract_minutes('minutes') }} as float) > 0
                then cast(distance as float) / (cast({{ extract_minutes('minutes') }} as float) / 60)
            end as miles_per_hour,

            case 
                when cast(touches as integer) > 0 and cast({{ extract_minutes('minutes') }} as float) > 0
                then cast(touches as float) / cast({{ extract_minutes('minutes') }} as float)
            end as touches_per_min,

            case 
                when cast(passes as integer) > 0 and cast({{ extract_minutes('minutes') }} as float) > 0
                then cast(passes as float) / cast({{ extract_minutes('minutes') }} as float)
            end as passes_per_min,

            case 
                when cast(reboundChancesTotal as integer) > 0
                then cast(reboundChancesOffensive as float) / cast(reboundChancesTotal as float)
            end as reb_chances_off_pct,

            case 
                when cast(contestedFieldGoalsAttempted as integer) + cast(uncontestedFieldGoalsAttempted as integer) > 0
                then cast(contestedFieldGoalsAttempted as float) / (cast(contestedFieldGoalsAttempted as float) + cast(uncontestedFieldGoalsAttempted as float))
            end as contested_shot_freq,

            case 
                when cast(passes as integer) > 0
                then (cast(assists as float) + cast(secondaryAssists as float) + cast(freeThrowAssists as float)) / cast(passes as float)
            end as pass_to_assist_pct,

             -- Game Stats - Calculated Percentages
        case 
            when cast("contestedShots" as integer) > 0 
            then cast("contestedShots2pt" as float) / cast("contestedShots" as float)
        end as cont_2pt_pct,
        case 
            when cast(contestedShots as integer) > 0 
            then cast(contestedShots3pt as float) / cast(contestedShots as float)
        end as cont_3pt_pct,
        case 
            when cast(boxOuts as integer) > 0 
            then cast(offensiveBoxOuts as float) / cast(boxOuts as float)
        end as off_box_outs_pct,

 -- Game Stats - Calculated Metrics
        case 
            when cast(matchupFieldGoalsAttempted as integer) > 0 
            then cast((matchupFieldGoalsMade + 0.5 * matchupThreePointersMade) as float) / cast(matchupFieldGoalsAttempted as float)
        end as matchup_efg_pct,

        case 
            when cast(helpFieldGoalsAttempted as integer) > 0 
            then cast((helpFieldGoalsMade + 0.5 * helpFieldGoalsMade) as float) / cast(helpFieldGoalsAttempted as float)
        end as help_efg_pct,

        case 
            when cast(matchupPotentialAssists as integer) > 0 
            then cast(matchupAssists as float) / cast(matchupPotentialAssists as float)
        end as ast_conversion_pct,

