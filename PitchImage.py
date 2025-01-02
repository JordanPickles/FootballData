from mplsoccer import Pitch

def pitch_image():
    # Define the horizontal pitch with the desired settings
    pitch = Pitch(
        pitch_type='statsbomb',
        corner_arcs=True,
        half=False,  # Draw the full pitch
        pitch_color='#555555',  # Background color of the pitch
        line_color='#c7d5cc',  # Line color on the pitch
        spot_type='square'  # Style of penalty spots
    )
    # Create the figure and axes
    fig, ax = pitch.draw(figsize=(16, 11), constrained_layout=True, tight_layout=False)
    # Set the figure background color
    fig.set_facecolor('#555555')

    # Save the pitch image
    fig.savefig(
        '/Users/jordanpickles/Library/CloudStorage/OneDrive-Personal/Personal Data Projects/FootballData/Football Pitch Horizontal.png',
        dpi=300,
        bbox_inches='tight'
    )

if __name__ == '__main__':
    pitch_image()