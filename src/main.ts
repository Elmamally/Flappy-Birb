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
    PIPE_SPEED: 2,
    RAND_VELOCITY: 3,
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
    isPass: boolean;
    svgElements: Readonly<{
        top: SVGElement;
        bottom: SVGElement;
    }>;
}>;

type PipeData = Readonly<{
    gap_y: number;
    gap_height: number;
    time: number;
}>;


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
    lives: number;
    score: number;
    lastGamePlay: ReadonlyArray<Readonly<{ y: number; time: number }>>;
    presentGamePlay: ReadonlyArray<Readonly<{ y: number; time: number }>>;
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
    lives: 3,
    score: 0,
    lastGamePlay: [],
    presentGamePlay: [],
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

const parseCsv = (csvInfo: string): ReadonlyArray<PipeData> => {
    const lines = csvInfo.trim().split('\n');
    return lines.slice(1)
        .filter(line => line.trim() !== '')
        .map((line, index) => {
            const [gapY, gapHeight, time] = line.split(',');
            return {
                gap_y: parseFloat(gapY),
                gap_height: parseFloat(gapHeight),
                time: parseFloat(time) * 1000, 
            };
        });
};

const pipeCreation = (pipeData: PipeData, index: number, svg: SVGSVGElement): Pipe => {
    const gapY = pipeData.gap_y * Viewport.CANVAS_HEIGHT;
    const gapHeight = pipeData.gap_height * Viewport.CANVAS_HEIGHT;

    const top = createSvgElement(svg.namespaceURI, "rect", {
        x: `${Viewport.CANVAS_WIDTH}`,
        y: "0",
        width: `${Constants.PIPE_WIDTH}`,
        height: `${gapY - gapHeight / 2}`,
        fill: "green",
    });

    const bottom = createSvgElement(svg.namespaceURI, "rect", {
        x: `${Viewport.CANVAS_WIDTH}`,
        y: `${gapY + gapHeight / 2}`,
        width: `${Constants.PIPE_WIDTH}`,
        height: `${Viewport.CANVAS_HEIGHT - (gapY + gapHeight / 2)}`,
        fill: "green",
    });

    svg.appendChild(top);
    svg.appendChild(bottom);

    return {
        id: `pipe-${index}`,
        x: Viewport.CANVAS_WIDTH,
        gapY,
        gapHeight,
        isPass: false,
        svgElements: { top, bottom }
    };
};

const pipesCollisionChecker = (bird_y: number, pipe: Pipe): boolean => {
    const leftBird = Birb.START_AT;
    const rightBird = Birb.START_AT + Birb.WIDTH;
    const topBird = bird_y;
    const bottomBird = bird_y + Birb.HEIGHT;

    const leftPipe = pipe.x;
    const rightPipe = pipe.x +Constants.PIPE_WIDTH;

    return (rightBird > leftPipe && leftBird< rightPipe) 
        ? (topBird < pipe.gapY - pipe.gapHeight / 2 || bottomBird > pipe.gapY + pipe.gapHeight / 2)
        : false; 
};

const boundaryCollisionChecker = (bird_y: number): boolean =>
    bird_y <= Constants.CEILING || bird_y + Birb.HEIGHT >= Constants.GROUND;

const pipesUpdator = (
    state: State,
    pipeDataArray: ReadonlyArray<PipeData>,
    svg: SVGSVGElement
): State => {
    const updatedPipes = state.pipes
        .map(pipe => {
            const newX = pipe.x - Constants.PIPE_SPEED;
            pipe.svgElements.top.setAttribute("x", `${newX}`);
            pipe.svgElements.bottom.setAttribute("x", `${newX}`);
            return { ...pipe, x: newX };
        })
        .filter(pipe => {
            const keep = pipe.x + Constants.PIPE_WIDTH > -50;
            if (!keep) {
                pipe.svgElements.top.remove();
                pipe.svgElements.bottom.remove();
            }
            return keep;
        });

    const newPipes = pipeDataArray
        .filter(pipeData =>
            pipeData.time <= state.time - state.gameStartTime &&
            !updatedPipes.some(pipe => pipe.id.endsWith(pipeDataArray.indexOf(pipeData).toString()))
        )
        .map((pipeData, index) =>
            pipeCreation(pipeData, pipeDataArray.indexOf(pipeData), svg)
        );

    return {
        ...state,
        pipes: [...updatedPipes, ...newPipes]
    };
};


const gameOverchecker = (state: State, pipeDataArray: ReadonlyArray<PipeData>): State => {
    const allPipesPassed = state.pipes.length > 0 && 
        state.pipes.every(pipe => pipe.passed) &&
        state.time - state.gameStartTime > Math.max(...pipeDataArray.map(p => p.time)) + 5000;
    
    if (allPipesPassed && state.gameState === "playing") {
        return { ...state, gameState: "gameOver" };
    }
    
    return state;
};

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
    const active = s.gameState === "playing";
    const velAfterGrav = s.velocity + Constants.GRAVITY;
    const yAfter = s.y + velAfterGrav;
    const ceilingCollision = active && velAfterGrav < 0 && yAfter <= Constants.CEILING;
    const groundCollision  = active && velAfterGrav > 0 && yAfter + Birb.HEIGHT >= Constants.GROUND;
    const pipeCollision = active && s.pipes.some(p => pipesCollisionChecker(yAfter, p));
    const allCollisions = ceilingCollision || groundCollision || pipeCollision;
    const livesUpdator = allCollisions ? s.lives - 1 : s.lives;
    const gameOverCon = livesUpdator <= 0 ? "gameOver" : s.gameState;
    const play = active
        ? [...s.presentGamePlay, {time: newTime - s.gameStartTime, y: yAfter}]
        : s.presentGamePlay;
    
    const newY =
      ceilingCollision ? Constants.CEILING :
      groundCollision  ? Constants.GROUND - Birb.HEIGHT :
      (active ? yAfter : s.y);
    const newVelocity =
      ceilingCollision ?  Math.abs(Constants.BOUNCE_SPEED) :
      groundCollision  ? -Math.abs(Constants.BOUNCE_SPEED) :
      (active ? velAfterGrav : s.velocity);
    const newBounces =
      s.bounces + (active && (ceilingCollision || groundCollision) ? 1 : 0);

    return {
      ...s,
      velocity: newVelocity,
      y: newY,
      bounces: newBounces,
      seed: s.seed,
      time: newTime,
      lives: livesUpdator,
      gameState: gameOverCon,
      presentGamePlay: gameOverCon === "gameOver" ? [] : play,
      lastGamePlay: gameOverCon === "gameOver" ? s.presentGamePlay : s.lastGamePlay,
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
  filter(event => event.code === "KeyR"),
  filter(event => !event.repeat),
  map(() => (s: State) =>
    s.gameState === "gameOver"
      ? { ...initialState, time: s.time, lastGamePlay: s.presentGamePlay.length > 0 ? s.presentGamePlay : s.lastGamePlay }
      : s
  )
);


const ghostBird = (
    play: ReadonlyArray<Readonly<{ y: number; time: number }>>,
    playTime: number
): number | null => {
    const history = play.filter(entry => entry.time <= playTime);
    return history.length > 0 ? history[history.length - 1].y : null;
};


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

    const ghostBirdImg: SVGElement = createSvgElement(svg.namespaceURI, "image", {
        href: "assets/birb.png",
        width: `${Birb.WIDTH}`,
        height: `${Birb.HEIGHT}`,
        opacity: "0.5",
        style: "filter: hue-rotate(120deg) brightness(0.7);",
    });
    svg.appendChild(ghostBirdImg);

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

        livesText.textContent = `${s.lives}`; 

        const playTime = s.gameState === "playing" ? s.time - s.gameStartTime :
        s.gameState === "gameOver" ? s.lastGamePlay.length > 0 ? s.lastGamePlay[s.lastGamePlay.length - 1].time : 0
        : 0;
        const ghostPos = ghostBird(s.lastGamePlay, playTime);

        ghostPos !== null && (s.gameState === "playing" || s.gameState === "gameOver")
         ? (
            ghostBirdImg.setAttribute("x", `${Birb.START_AT - Birb.WIDTH / 2}`),
            ghostBirdImg.setAttribute(
                "y",
                `${Math.max(0, Math.min(ghostPos, Viewport.CANVAS_HEIGHT - Birb.HEIGHT))}`
            ),
            ghostBirdImg.style.visibility = "visible"
            )
         : (ghostBirdImg.style.visibility = "hidden");

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
