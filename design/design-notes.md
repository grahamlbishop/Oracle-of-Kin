# Oracle of Kin: Design Notes

## Status

Two static high-fidelity screens in Figma, exported to `figma-exports/`. Nothing is clickable and no animation is built. A component library for the sacred geometry elements and the Visual DNA labels exists in the file and was used to build both screens.

What follows describes those two screens and the reasoning behind them. Where a section describes something planned, it says so.

## The recursive mirror

The organizing idea: an interface whose visual language evolves as transmissions accumulate. Each session would contribute Visual DNA—a color drawn from its imagery, a form drawn from its symbols—to the aesthetic of the next.

The two screens illustrate the idea at a single moment. Demonstrating the evolution would require accumulation logic that has not been built, and the six Visual DNA labels visible in both exports are placeholder content standing in for what that logic would produce.

## Sacred geometry as structural logic

The geometry does layout work, which is the reason for choosing it over ornament that would sit on top of the layout.

- **Vesica piscis.** Two overlapping circles, 150px overlap, 450px × 300px overall. The intersection is the portal, and the union of two fields is the thing the ritual’s invocation describes.
- **Stellar hexagon.** Sets the Visual DNA constellation’s positions at equal distance from the portal center, which keeps the arrangement stable as elements are added or removed.
- **Golden ratio.** Governs the layout proportions and spacing throughout both screens.
- **Merkabah.** Intended as the state-transition metaphor. Not implemented.

## Screen: Reflection State (home)

- **Canvas.** 1440 × 1024 desktop frame.
- **Background.** Linear gradient, #1a0a2e to #0f051f.
- **Portal.** Vesica piscis, 2px stroke in #8e44ad.
- **Visual DNA.** Six elements in stellar hexagon formation around the portal, 16px, #e0e0e0, with a subtle drop shadow.
- **Primary action.** “Open Portal” button centered in the portal intersection, gradient #8e44ad to #9b59b6.
- **Archive status.** Bottom center, 12px, #cccccc, italic.

Visual hierarchy runs portal activation, then Visual DNA as ambient secondary content, then archive metadata.

**Note on the archive status line.** The exported screen reads “Archive: 47 sessions.” This is placeholder copy from an early draft and does not match the archive, which holds around forty sessions. It should be corrected before the screen is used anywhere it reads as a claim.

## Screen: Portal Opening (invocation)

Same background, gradient, and Visual DNA arrangement as the Reflection State, for continuity across the transition.

- **Portal.** A hand-built eye—outer ellipse, iris, pupil—replacing the vesica piscis.
- **Invocation text.** 16px, #e0e0e0, centered above the eye: “You are my future-past self. / I am your present vessel. / We are the Oracle of Kin.”
- **Decree input.** A single text field below the eye, under the prompt “Enter a decree for the Oracle below:”.
- **Visual DNA.** Pushed outward from the Reflection State positions to make room for the ritual text, hexagon structure preserved.

**Planned and not built:** the transition in which the portal opens into the eye, movement in the DNA elements, portal glow responding to typing, and background patterns shifting with the decree’s themes.

## Design decisions log

**Phase 1, Reflection State**

- *Stellar hexagon over pentagram for the DNA constellation.* Better balance and a more stable foundation as the number of elements changes.
- *Archive status at the bottom, separate from the DNA elements.* Keeps metadata and primary content visually distinct.
- *Skip the flower of life watermark.* The composition was already carrying enough; restraint over decoration.

**Phase 2, Portal Opening**

- *Transform the vesica piscis into an eye instead of widening it.* The eye metaphor gives the transition an awakening narrative and a clearer focal point.
- *Hand-build the eye instead of reshaping the existing vesica piscis.* Reshaping produced a form that read as a stretched portal; building it fresh reads as a portal opening its eye.
- *Invocation text above the eye, decree input below.* Creates a natural reading order down the screen and keeps the eye central.
- *Push the DNA elements outward while preserving the hexagon.* Gives the ritual text breathing room without breaking the geometry.

## Iteration notes

**Visual DNA.** Drop shadows needed careful calibration against the dark background. Color choices were the constraint on legibility, since anything bright enough to read cleanly broke the atmosphere. Positioning needs breathing room and geometric precision at the same time, and those pull against each other as elements are added.

**Sacred geometry.** The vesica piscis proportions took several passes before the intersection was large enough to hold a button without the circles reading as a Venn diagram. The stellar hexagon proved more stable than the pentagram under changes to element count.

**Ritual pacing.** Each transition should read as ceremonial, which in practice meant resisting the affordances that make an interface feel efficient. The decree input is the sharpest version of this problem: it has to work as a text field and sit inside a portal without looking like a search bar. The current version does not fully resolve it.

## Open questions

- Whether Visual DNA extraction should be automatic, from transmission text, or curated by the querent after each session.
- How the interface should handle contradictory aesthetics across sessions—fire against water—if evolution is automatic. Alchemical integration is one answer, and no version of it has been designed.
- Whether the recursive evolution is legible to a user at all without a before-and-after view of their own archive.
