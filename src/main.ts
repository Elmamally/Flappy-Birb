/**
 * Inside this file you will use the classes and functions from rx.js
 * to add visuals to the svg element in index.html, animate them, and make them interactive.
 *
 * Study and complete the tasks in observable exercises first to get ideas.
 *
 * Course Notes showing Asteroids in FRP: https://tgdwyer.github.io/asteroids/
 *
 * You will be marked on your functional programming style
 * as well as the functionality that you implement.
 *
 * Document your code!
 */

import { constants } from "buffer";
import "./style.css";

import {
    Observable,
    catchError,
    filter,
    fromEvent,
    interval,
    map,
    merge,
    scan,
    startWith,
    switchMap,
    take,
    takeWhile,
} from "rxjs";
import { fromFetch } from "rxjs/fetch";
import { parse } from "path";


export function createRngStreamFromSource<T>(source$: Observable<T>) {
    return function createRngStream(
        seed: number = 0,
    ): Observable<number> {
        const randomNumberStream = source$.pipe(
            scan((currentSeed, _) => RNG.hash(currentSeed), seed),
            map(hash => RNG.scale(hash))
        );

        return randomNumberStream;
    };
}

/** Constants */

const Viewport = {
    CANVAS_WIDTH: 600,
    CANVAS_HEIGHT: 400,
} as const;

const Birb = {
    WIDTH: 42,
    HEIGHT: 30,
    START_AT: 180,
} as const;

const Constants = {
    PIPE_WIDTH: 50,
    TICK_RATE_MS: 20, // Might need to change this!
    GRAVITY: 0.5,
    GROUND: Viewport.CANVAS_HEIGHT,
    SEED: 1234,
    FLAP_VELOCITY: -6,
    CEILING: 0,
    BOUNCE_SPEED: 6,
    BOUNCE_HEIGHT: -8,
} as const;

// User input

type Key = "Space";

type GameState = "waiting" | "playing" | "gameOver";

// Pipe
type Pipe = Readonly<{
    id: String; 
    x: number;
    gapY: number;
    gapHeight: number;
    svgElements: Readonly<{
        top: SVGElement;
        bottom: SVGElement;
    }>;
}>;

type PipeData = Readonly<{
    gap_y: number;
    gap_height: number;
    time: number;
}>


// State processing

type State = Readonly<{
    gameState: GameState;
    pipes: ReadonlyArray<Pipe>;
    y: number;
    velocity: number;
    bounces: number;
    seed: number;
    time: number;
    gameStartTime: number;
}>;

const initialState: State = {
    gameState: "waiting",
    pipes: [],
    y: Constants.GROUND - 80,
    velocity: 0,
    bounces: 0,
    seed: Constants.SEED,
    time: 0,
    gameStartTime: 0,
};

//copied from applied 4
abstract class RNG {
    private static m = 0x80000000; // 2^31
    private static a = 1103515245;
    private static c = 12345;

    public static hash = (seed: number): number =>
        (RNG.a * seed + RNG.c) % RNG.m;

    public static scale = (hash: number): number =>
        (2 * hash) / (RNG.m - 1) - 1; // in [-1, 1]
}

const boundaryCollisionChecker = (bird_y: number): boolean =>
    bird_y <= Constants.CEILING || bird_y + Birb.HEIGHT >= Constants.GROUND;


const jump$: Observable<(s: State) => State> = fromEvent<KeyboardEvent>(
        document,
        "keydown",
    ).pipe(
        filter(event => event.code === "Space"),
        map(() => (s: State) => {
            const newHash = RNG.hash(s.seed);        
            const rand = RNG.scale(newHash);         
            const velocity = Constants.FLAP_VELOCITY;            
            return {
                ...s,
                velocity,
                seed: newHash,                          
            };
        })
    );

/**
 * Updates the state by proceeding with one time step.
 *
 * @param s Current state
 * @returns Updated state
 */
const tick$: Observable<(s: State) => State> = interval(Constants.TICK_RATE_MS).pipe(
  map(() => (s: State) => {
    const newTime = s.time + Constants.TICK_RATE_MS;

    // game is active flag
    const active = s.gameState === "playing";

    // velocity after gravity
    const vAfterGravity = s.velocity + Constants.GRAVITY;
    const yAfter = s.y + vAfterGravity;

    // collisions
    const hitCeiling = active && vAfterGravity < 0 && yAfter <= Constants.CEILING;
    const hitGround  = active && vAfterGravity > 0 && yAfter + Birb.HEIGHT >= Constants.GROUND;

    // clamp Y
    const newY =
      hitCeiling ? Constants.CEILING :
      hitGround  ? Constants.GROUND - Birb.HEIGHT :
      (active ? yAfter : s.y);

    // constant bounce velocity
    const newVelocity =
      hitCeiling ?  Math.abs(Constants.BOUNCE_SPEED) :
      hitGround  ? -Math.abs(Constants.BOUNCE_SPEED) :
      (active ? vAfterGravity : s.velocity);

    // count bounces only while active
    const newBounces =
      s.bounces + (active && (hitCeiling || hitGround) ? 1 : 0);

    return {
      ...s,
      velocity: newVelocity,
      y: newY,
      bounces: newBounces,
      seed: s.seed,
      time: newTime,
      gameState: s.gameState,
    };
  })
);



const gameStart$: Observable<(s: State) => State> = fromEvent<KeyboardEvent>(document, "keydown").pipe(
    filter(event => event.code ==="Space"),
    map(() => (s: State) => {
        return s.gameState ==="waiting" ? {...s, gameState: "playing", startTime: s.time}: s;
    })
);

const gameRestart$: Observable<(s: State) => State> = fromEvent<KeyboardEvent>(document, "keydown").pipe(
    filter(event => event.code ==="KeyR"),
    map(() => (s: State) => {
        return s.gameState ==="gameOver" ? {...initialState, time: s.time}: s;
    })
);

// Rendering (side effects)

/**
 * Brings an SVG element to the foreground.
 * @param elem SVG element to bring to the foreground
 */
const bringToForeground = (elem: SVGElement): void => {
    elem.parentNode?.appendChild(elem);
};

/**
 * Displays a SVG element on the canvas. Brings to foreground.
 * @param elem SVG element to display
 */
const show = (elem: SVGElement): void => {
    elem.setAttribute("visibility", "visible");
    bringToForeground(elem);
};

/**
 * Hides a SVG element on the canvas.
 * @param elem SVG element to hide
 */
const hide = (elem: SVGElement): void => {
    elem.setAttribute("visibility", "hidden");
};

/**
 * Creates an SVG element with the given properties.
 *
 * See https://developer.mozilla.org/en-US/docs/Web/SVG/Element for valid
 * element names and properties.
 *
 * @param namespace Namespace of the SVG element
 * @param name SVGElement name
 * @param props Properties to set on the SVG element
 * @returns SVG element
 */
const createSvgElement = (
    namespace: string | null,
    name: string,
    props: Record<string, string> = {},
): SVGElement => {
    const elem = document.createElementNS(namespace, name) as SVGElement;
    Object.entries(props).forEach(([k, v]) => elem.setAttribute(k, v));
    return elem;
};

const render = (): ((s: State) => void) => {
    // Canvas elements
    const gameOver = document.querySelector("#gameOver") as SVGElement;
    const container = document.querySelector("#main") as HTMLElement;

    // Text fields
    const livesText = document.querySelector("#livesText") as HTMLElement;
    const scoreText = document.querySelector("#scoreText") as HTMLElement;

    const svg = document.querySelector("#svgCanvas") as SVGSVGElement;

    svg.setAttribute(
        "viewBox",
        `0 0 ${Viewport.CANVAS_WIDTH} ${Viewport.CANVAS_HEIGHT}`,
    );

    const birdImg: SVGElement = createSvgElement(svg.namespaceURI, "image", {
        href: "assets/birb.png",
        width: `${Birb.WIDTH}`,
        height: `${Birb.HEIGHT}`,
    });
    svg.appendChild(birdImg);

    /**
     * Renders the current state to the canvas.
     *
     * In MVC terms, this updates the View using the Model.
     *
     * @param s Current state
     */
    return (s: State) => {
        birdImg.setAttribute("x", `${Birb.START_AT - Birb.WIDTH / 2}`);
        birdImg.setAttribute(
        "y",
        `${Math.max(0, Math.min(s.y, Viewport.CANVAS_HEIGHT - Birb.HEIGHT))}`,
        );


        // Draw a static pipe as a demonstration
        const pipeGapY = 200; // vertical center of the gap
        const pipeGapHeight = 100;

        // Top pipe
        const pipeTop = createSvgElement(svg.namespaceURI, "rect", {
            x: "150",
            y: "0",
            width: `${Constants.PIPE_WIDTH}`,
            height: `${pipeGapY - pipeGapHeight / 2}`,
            fill: "green",
        });

        // Bottom pipe
        const pipeBottom = createSvgElement(svg.namespaceURI, "rect", {
            x: "150",
            y: `${pipeGapY + pipeGapHeight / 2}`,
            width: `${Constants.PIPE_WIDTH}`,
            height: `${Viewport.CANVAS_HEIGHT - (pipeGapY + pipeGapHeight / 2)}`,
            fill: "green",
        });

        //svg.appendChild(pipeTop);
        //svg.appendChild(pipeBottom);
    };
};

export const state$ = (csvContents: string): Observable<State> => {
    return merge(jump$, tick$, gameRestart$, gameStart$).pipe(
        startWith((s: State) => s), 
        scan((state, update) => update(state), initialState),
        takeWhile(() => true, true) 
    );
};


// The following simply runs your main function on window load.  Make sure to leave it in place.
// You should not need to change this, beware if you are.
if (typeof window !== "undefined") {
    const { protocol, hostname, port } = new URL(import.meta.url);
    const baseUrl = `${protocol}//${hostname}${port ? `:${port}` : ""}`;
    const csvUrl = `${baseUrl}/assets/map.csv`;

    // Get the file from URLa
    const csv$ = fromFetch(csvUrl).pipe(
        switchMap(response => {
            if (response.ok) {
                return response.text();
            } else {
                throw new Error(`Fetch error: ${response.status}`);
            }
        }),
        catchError(err => {
            console.error("Error fetching the CSV file:", err);
            throw err;
        }),
    );

    // Observable: wait for first user click
    const click$ = fromEvent(document.body, "mousedown").pipe(take(1));

    csv$.pipe(
        switchMap(contents =>
            // On click - start the game
            click$.pipe(switchMap(() => state$(contents))),
        ),
    ).subscribe(render());
}
