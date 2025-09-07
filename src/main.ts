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

import "./style.css";

import {
    Observable,
    catchError,
    filter,
    fromEvent,
    interval,
    map,
    scan,
    switchMap,
    take,
    merge,
    startWith,
    takeWhile,
    distinctUntilChanged,
    combineLatest,
    EMPTY,
    of,
} from "rxjs";
import { fromFetch } from "rxjs/fetch";

/** Constants */
const Viewport = {
    CANVAS_WIDTH: 600,
    CANVAS_HEIGHT: 400,
} as const;

const Birb = {
    WIDTH: 42,
    HEIGHT: 30,
    START_X: Viewport.CANVAS_WIDTH * 0.3,
} as const;

const Constants = {
    PIPE_WIDTH: 50,
    TICK_RATE_MS: 20,
    GRAVITY: 0.5,
    GROUND: Viewport.CANVAS_HEIGHT,
    CEILING: 0,
    SEED: 1234,
    PIPE_SPEED: 4,
    FLAP_VELOCITY: -5,
    INITIAL_LIVES: 3,
    BOUNCE_DAMPING: 0.6,
    RANDOM_VELOCITY_RANGE: 3,
} as const;

// User input
type Key = "Space";

type Pipe = Readonly<{
    id: string;
    x: number;
    gapY: number;
    gapHeight: number;
    passed: boolean;
    svgElements?: Readonly<{
        top: SVGElement;
        bottom: SVGElement;
    }>;
}>;

type GameState = "playing" | "gameOver" | "waiting" | "paused";

type State = Readonly<{
    gameState: GameState;
    pipes: ReadonlyArray<Pipe>;
    y: number;
    velocity: number;
    lives: number;
    score: number;
    seed: number;
    time: number;
    gameStartTime: number;
    previousGameHistory: ReadonlyArray<ReadonlyArray<Readonly<{ y: number; time: number }>>>;
    currentGameHistory: ReadonlyArray<Readonly<{ y: number; time: number }>>;
    isCollidingPipeId: string | null;
}>;

type PipeData = Readonly<{
    gap_y: number;
    gap_height: number;
    time: number;
}>;

// Random Number Generator, coppied from applied 4
abstract class RNG {
    private static m = 0x80000000; // 2^31
    private static a = 1103515245;
    private static c = 12345;

    public static hash = (seed: number): number =>
        (RNG.a * seed + RNG.c) % RNG.m;

    public static scale = (hash: number): number =>
        (2 * hash) / (RNG.m - 1) - 1; // in [-1, 1]
}

const initialState: State = {
    gameState: "waiting",
    pipes: [],
    y: Constants.GROUND - 80,
    velocity: 0,
    lives: Constants.INITIAL_LIVES,
    score: 0,
    seed: Constants.SEED,
    time: 0,
    gameStartTime: 0,
    previousGameHistory: [],
    currentGameHistory: [],
    isCollidingPipeId: null,
};

const parsePipeData = (csvContent: string): ReadonlyArray<PipeData> => {
    const lines = csvContent.trim().split("\n").slice(1);

    const rawPipes: PipeData[] = lines
        .filter((line) => line.trim() !== "")
        .map((line) => {
        const [gapY, gapHeight, time] = line.split(",");
        return {
            gap_y: parseFloat(gapY),
            gap_height: parseFloat(gapHeight),
            time: parseFloat(time) * 1000, 
        };
        });

    const MINIMUM_PIPE_SPACING = 2000; 
    const filtered: PipeData[] = [];

    rawPipes.forEach((pipe) => {
        !filtered.length || pipe.time - filtered[filtered.length - 1].time >= MINIMUM_PIPE_SPACING
        ? filtered.push(pipe)
        : null;
    });

    return filtered;
};

const createPipe = (pipeData: PipeData, index: number, x: number = Viewport.CANVAS_WIDTH): Pipe => ({
  id: `pipe-${index}`,
  x,
  gapY: pipeData.gap_y * Viewport.CANVAS_HEIGHT,
  gapHeight: pipeData.gap_height * Viewport.CANVAS_HEIGHT,
  passed: false,
});

const checkBirdPipeCollision = (birdY: number, pipe: Pipe): boolean => {
  const padX = 6, padY = 4; 

  const birdLeft = Birb.START_X + padX;
  const birdRight = Birb.START_X + Birb.WIDTH - padX;
  const birdTop = birdY + padY;
  const birdBottom = birdY + Birb.HEIGHT - padY;

  const pipeLeft = pipe.x;
  const pipeRight = pipe.x + Constants.PIPE_WIDTH;

  const gapTop = pipe.gapY - pipe.gapHeight / 2;
  const gapBottom = pipe.gapY + pipe.gapHeight / 2;

  return birdRight >= pipeLeft && birdLeft <= pipeRight && (birdTop < gapTop || birdBottom > gapBottom);
};


const checkBirdBoundaryCollision = (birdY: number): boolean =>
    birdY <= Constants.CEILING || birdY + Birb.HEIGHT >= Constants.GROUND;

const calculateBounceVelocity = (
    birdY: number,
    pipe: Pipe | null,
    seed: number
): { velocity: number; newSeed: number } => {
    const newHash = RNG.hash(seed);
    const randomOffset = RNG.scale(newHash) * Constants.RANDOM_VELOCITY_RANGE;

    const baseVelocity =
        pipe
            ? (() => {
                  const gapCenter = pipe.gapY;
                  return birdY < gapCenter || birdY <= Constants.CEILING + Birb.HEIGHT
                      ? Math.abs(Constants.FLAP_VELOCITY) * Constants.BOUNCE_DAMPING
                      : -Math.abs(Constants.FLAP_VELOCITY) * Constants.BOUNCE_DAMPING;
              })()
            : birdY <= Constants.CEILING + Birb.HEIGHT
            ? Math.abs(Constants.FLAP_VELOCITY) * Constants.BOUNCE_DAMPING
            : -Math.abs(Constants.FLAP_VELOCITY) * Constants.BOUNCE_DAMPING;

    return {
        velocity: baseVelocity + randomOffset,
        newSeed: newHash,
    };
};

const updateBirdPhysics = (state: State): State => ({
    ...state,
    velocity: state.velocity + Constants.GRAVITY,
    y: state.y + state.velocity,
    currentGameHistory: [...state.currentGameHistory, { y: state.y, time: state.time }],
});

const updatePipes = (state: State, pipeDataArray: ReadonlyArray<PipeData>): State => {
    const updatedPipes = state.pipes
        .map((pipe) => ({ ...pipe, x: pipe.x - Constants.PIPE_SPEED }))
        .filter((pipe) => pipe.x + Constants.PIPE_WIDTH > -50);

    const elapsed = state.time - state.gameStartTime; 
    const speedPxPerMs = Constants.PIPE_SPEED / Constants.TICK_RATE_MS; 

    const newPipesToAdd = pipeDataArray
        .map((pd, idx) => ({ pd, idx }))
        .filter(({ pd }) => pd.time <= elapsed)
        .filter(({ idx }) => !updatedPipes.some((p) => p.id === `pipe-${idx}`))
        .map(({ pd, idx }) => {
        const deltaMs = Math.max(0, elapsed - pd.time);
        const offsetPx = Math.round(deltaMs * speedPxPerMs);
        const x = Viewport.CANVAS_WIDTH - offsetPx;
        return createPipe(pd, idx, x);
        })
        .filter((pipe) => pipe.x + Constants.PIPE_WIDTH > -50);

    return {
        ...state,
        pipes: [...updatedPipes, ...newPipesToAdd],
    };
};

const updateScore = (state: State): State => {
  const newPipes = state.pipes.map((pipe) =>
    !pipe.passed && pipe.x + Constants.PIPE_WIDTH < Birb.START_X ? { ...pipe, passed: true } : pipe
  );

  const newlyPassedPipes = newPipes.filter(
    (p) => p.passed && !state.pipes.some((prev) => prev.id === p.id && prev.passed)
  ).length;

  return {
    ...state,
    pipes: newPipes,
    score: state.score + newlyPassedPipes,
  };
};

const checkCollisions = (state: State): State => {
  const collidingPipe = state.pipes.find((pipe) => checkBirdPipeCollision(state.y, pipe));
  const boundaryCollision = checkBirdBoundaryCollision(state.y);

  const collisionId = collidingPipe ? collidingPipe.id : boundaryCollision ? "boundary" : null;
  const collisionNow = !!collisionId;

  return collisionNow && state.isCollidingPipeId !== collisionId
    ? (() => {
        const { velocity, newSeed } = calculateBounceVelocity(state.y, collidingPipe || null, state.seed);
        const newLives = state.lives - 1;
        return {
          ...state,
          velocity,
          seed: newSeed,
          lives: newLives,
          isCollidingPipeId: collisionId,
          gameState: newLives <= 0 ? "gameOver" : state.gameState,
        };
      })()
    : !collisionNow && state.isCollidingPipeId !== null
    ? { ...state, isCollidingPipeId: null }
    : state;
};


const checkGameEnd = (state: State, pipeDataArray: ReadonlyArray<PipeData>): State => {
    const lastPipeTime = Math.max(...pipeDataArray.map((p) => p.time));

    const finishedAll =
        state.time - state.gameStartTime > lastPipeTime + 5000 &&
        state.pipes.every((p) => p.passed);

    return finishedAll && state.gameState === "playing"
        ? { ...state, gameState: "gameOver" }
        : state;
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
    //Canvas elements
    const gameOver = document.querySelector("#gameOver") as SVGElement | null;
    const livesText = document.querySelector("#livesText") as HTMLElement | null;

    //Text fields
    const scoreText = document.querySelector("#scoreText") as HTMLElement | null;
    const svg = document.querySelector("#svgCanvas") as SVGSVGElement;

    svg.setAttribute("viewBox", `0 0 ${Viewport.CANVAS_WIDTH} ${Viewport.CANVAS_HEIGHT}`);

    const birdRef: { current: SVGElement | null } = { current: null };
    const ghostMap: Map<number, SVGElement> = new Map();
    const pipeElements = new Map<string, { top: SVGElement; bottom: SVGElement }>();

    /**
     * Renders the current state to the canvas.
     *
     * In MVC terms, this updates the View using the Model.
     *
     * @param s Current state
     */

    return (s: State) => {
        livesText && (livesText.textContent = `${s.lives}`);
        scoreText && (scoreText.textContent = `${s.score}`);
        gameOver && (gameOver.style.visibility = s.gameState === "gameOver" ? "visible" : "hidden");

    birdRef.current ||
      ((() => {
        const el = createSvgElement(svg.namespaceURI, "image", {
          href: "assets/birb.png",
          width: `${Birb.WIDTH}`,
          height: `${Birb.HEIGHT}`,
        });
        svg.appendChild(el);
        birdRef.current = el;
      })());

    birdRef.current!.setAttribute("x", `${Birb.START_X - Birb.WIDTH / 2}`);
    birdRef.current!.setAttribute("y", `${Math.max(0, Math.min(s.y, Viewport.CANVAS_HEIGHT - Birb.HEIGHT))}`);

const gameTime = s.time - s.gameStartTime;

    s.previousGameHistory.forEach((run, runIndex) => {
        const existing = ghostMap.get(runIndex);
        existing ||
            (() => {
            const g = createSvgElement(svg.namespaceURI, "image", {
                href: "assets/birb.png",
                width: `${Birb.WIDTH}`,
                height: `${Birb.HEIGHT}`,
                opacity: `${Math.max(0.15, 0.6 - runIndex * 0.15)}`,
                style: "filter: hue-rotate(120deg) brightness(0.7);",
            });
            svg.appendChild(g);
            ghostMap.set(runIndex, g);
            })();

        const gEl = ghostMap.get(runIndex)!;
        const lastRecordedTime = run.length > 0 ? run[run.length - 1].time : 0;

        s.gameState === "playing"
            ? (() => {
                const ghostPos = run.find((pos, i) => {
                    const next = run[i + 1];
                    return pos.time <= gameTime && (!next || next.time > gameTime);
                });

                (ghostPos && gameTime < lastRecordedTime)
                    ? (gEl.setAttribute("x", `${Birb.START_X - Birb.WIDTH / 2}`),
                    gEl.setAttribute("y", `${Math.max(0, Math.min(ghostPos.y, Viewport.CANVAS_HEIGHT - Birb.HEIGHT))}`),
                    (gEl.style.visibility = "visible"))
                    : (gEl.style.visibility = "hidden");
            })()
            : (gEl.style.visibility = "hidden");
    });

    const removeKeys = Array.from(ghostMap.keys()).filter((k) => k >= s.previousGameHistory.length);
    removeKeys.forEach((k) => {
        const el = ghostMap.get(k)!;
        el.remove();
        ghostMap.delete(k);
    });

    s.pipes.forEach((pipe) => {
    pipeElements.has(pipe.id) ||
        (() => {
        const topPipe = createSvgElement(svg.namespaceURI, "rect", {
            fill: "green",
            width: `${Constants.PIPE_WIDTH}`,
        });
        const bottomPipe = createSvgElement(svg.namespaceURI, "rect", {
            fill: "green",
            width: `${Constants.PIPE_WIDTH}`,
        });
        pipeElements.set(pipe.id, { top: topPipe, bottom: bottomPipe });
        svg.appendChild(topPipe);
        svg.appendChild(bottomPipe);
        })();

    const elements = pipeElements.get(pipe.id)!;
    const gapTop = pipe.gapY - pipe.gapHeight / 2;
    const gapBottom = pipe.gapY + pipe.gapHeight / 2;
    
    elements.top.setAttribute("x", `${pipe.x}`);
    elements.top.setAttribute("y", "0");
    elements.top.setAttribute("height", `${gapTop}`);

    elements.bottom.setAttribute("x", `${pipe.x}`);
    elements.bottom.setAttribute("y", `${gapBottom}`);
    elements.bottom.setAttribute("height", `${Viewport.CANVAS_HEIGHT - gapBottom}`);
    });

    pipeElements.forEach((elements, pipeId) => {
      !s.pipes.some((pipe) => pipe.id === pipeId) &&
        (elements.top.remove(), elements.bottom.remove(), pipeElements.delete(pipeId));
    });
  };
};

export const state$ = (csvContents: string): Observable<State> => {
    const pipeDataArray = parsePipeData(csvContents);

    const jump$ = fromEvent<KeyboardEvent>(document, "keydown").pipe(
        filter((event) => event.code === "Space"),
        map(
        () =>
            (state: State): State =>
            state.gameState === "playing"
                ? ((newHash) => ({
                    ...state,
                    velocity: Constants.FLAP_VELOCITY + RNG.scale(newHash) * Constants.RANDOM_VELOCITY_RANGE,
                    seed: newHash,
                }))(RNG.hash(state.seed))
                : state
        )
    );

    const tick$ = interval(Constants.TICK_RATE_MS).pipe(
        map(
            () =>
            (state: State): State =>
                state.gameState !== "playing"
                ? state // paused, waiting, or gameOver → no updates
                : ((newTime: number) =>
                    [updateBirdPhysics,
                    (s: State) => updatePipes(s, pipeDataArray),
                    updateScore,
                    (s: State) => checkCollisions(s),
                    (s: State) => checkGameEnd(s, pipeDataArray)]
                    .reduce((acc, fn) => fn(acc), { ...state, time: newTime })
                    )(state.time + Constants.TICK_RATE_MS)
        )
        );


    const startGame$ = fromEvent<KeyboardEvent>(document, "keydown").pipe(
        filter((event) => event.code === "Space"),
        map(
        () =>
            (state: State): State =>
            state.gameState === "waiting"
                ? { ...state, gameState: "playing", gameStartTime: state.time, currentGameHistory: [] }
                : state
        )
    );

    const restart$ = fromEvent<KeyboardEvent>(document, "keydown").pipe(
        filter((event) => event.code === "KeyR"),
        map(
        () =>
            (state: State): State =>
            state.gameState === "gameOver"
                ? {
                    ...initialState,
                    previousGameHistory: [
                    ...state.previousGameHistory,
                    state.currentGameHistory.map((p) => ({ y: p.y, time: p.time - state.gameStartTime })),
                    ],
                    time: state.time,
                }
                : state
        )
    );

    const pause$ = fromEvent<KeyboardEvent>(document, "keydown").pipe(
        filter((event) => event.code === "KeyE"),
        map(
            () =>
            (state: State): State =>
                state.gameState === "playing"
                ? { ...state, gameState: "paused" }
                : state.gameState === "paused"
                ? { ...state, gameState: "playing" }
                : state
        )
    )
    return merge(jump$, tick$, startGame$, restart$, pause$).pipe(
        scan<(s: State) => State, State>((acc: State, update: (s: State) => State) => update(acc), initialState),
        startWith(initialState),
        takeWhile((_: State) => true, true)
    );
};


// The following simply runs your main function on window load.  Make sure to leave it in place.
// You should not need to change this, beware if you are.
if (typeof window !== "undefined") {
    const { protocol, hostname, port } = new URL(import.meta.url);
    const baseUrl = `${protocol}//${hostname}${port ? `:${port}` : ""}`;
    const csvUrl = `${baseUrl}/assets/map.csv`;

    // Get the file from URL
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
