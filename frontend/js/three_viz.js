import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { EffectComposer } from 'three/addons/postprocessing/EffectComposer.js';
import { RenderPass } from 'three/addons/postprocessing/RenderPass.js';
import { UnrealBloomPass } from 'three/addons/postprocessing/UnrealBloomPass.js';
import { OutputPass } from 'three/addons/postprocessing/OutputPass.js';

export function initThreeJS(canvasId) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;

    let W = window.innerWidth;
    let H = window.innerHeight;

    const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(W, H);
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 0.88;

    const scene = new THREE.Scene();
    scene.background = null; // transparent to show app background
    
    const camera = new THREE.PerspectiveCamera(50, W/H, 0.1, 600);
    camera.position.set(2, 5, 30);

    const controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true; 
    controls.dampingFactor = 0.06;
    controls.minDistance = 10; 
    controls.maxDistance = 65;
    controls.maxPolarAngle = Math.PI * 0.8; 
    controls.minPolarAngle = Math.PI * 0.08;
    controls.autoRotate = true; 
    controls.autoRotateSpeed = 0.28;

    const composer = new EffectComposer(renderer);
    composer.addPass(new RenderPass(scene, camera));
    const bloom = new UnrealBloomPass(new THREE.Vector2(W, H), 1.3, 0.42, 0.09);
    composer.addPass(bloom);
    composer.addPass(new OutputPass());

    // Lights
    scene.add(new THREE.AmbientLight(0x080820, 4));
    const cogLight = new THREE.PointLight(0xbf5af2, 18, 35); 
    scene.add(cogLight);

    // Nodes
    const C = {
        GIA:0x00d4ff, COG:0xbf5af2, DTB:0x30d158, SVB:0xff453a, THA:0xffd60a,
        DSW:[0x3b82f6,0x6366f1,0x30d158,0x00d4ff,0xf472b6,0xfbbf24,0x34d399,0x818cf8,0xfb923c,0x4ade80,0xa78bfa]
    };
    
    const nodeMeshes = [];
    const nodeMap = {};
    const dswObjects = [];

    function makeNode(data, size=1) {
        let geo;
        switch(data.shape) {
            case 'torusknot': geo = new THREE.TorusKnotGeometry(0.72*size, 0.26*size, 120, 18); break;
            case 'icosahedron': geo = new THREE.IcosahedronGeometry(1.25*size, 1); break;
            case 'torus': geo = new THREE.TorusGeometry(0.72*size, 0.3*size, 16, 60); break;
            case 'octahedron': geo = new THREE.OctahedronGeometry(0.9*size, 0); break;
            default: geo = new THREE.SphereGeometry(0.65*size, 32, 32);
        }
        const mat = new THREE.MeshStandardMaterial({
            color: data.col, emissive: data.col, emissiveIntensity: 0.5, metalness: 0.25, roughness: 0.2, wireframe: data.shape==='wireframe'
        });
        const mesh = new THREE.Mesh(geo, mat);
        mesh.position.set(...data.pos); 
        scene.add(mesh); 
        nodeMeshes.push(mesh);
        
        const pl = new THREE.PointLight(data.col, 4, 14); 
        pl.position.set(...data.pos); 
        scene.add(pl);
        
        const glow = new THREE.Mesh(new THREE.SphereGeometry(2*size, 16, 16), new THREE.MeshBasicMaterial({color: data.col, transparent: true, opacity: 0.04, side: THREE.BackSide}));
        glow.position.set(...data.pos); 
        scene.add(glow);
        
        nodeMap[data.id] = {mesh, light: pl, glow};
    }

    const NODES = [
        {id:'GIA', pos:[-11,0,0], col:C.GIA, shape:'torusknot'},
        {id:'COG', pos:[0,0,0], col:C.COG, shape:'icosahedron'},
        {id:'DTB', pos:[8,-2,3], col:C.DTB, shape:'torus'},
        {id:'SVB', pos:[4,-5,-1.5], col:C.SVB, shape:'octahedron'},
        {id:'THA', pos:[-3,6,-2.5], col:C.THA, shape:'wireframe'},
    ];
    NODES.forEach(d => makeNode(d, d.id === 'COG' ? 1.45 : 1));

    for(let i=0; i<11; i++){
        const ang = (i/11) * Math.PI * 2;
        const R = 5.6;
        const yo = Math.sin(i * 0.85) * 2.3;
        makeNode({id:`DSW${i}`, pos:[Math.cos(ang)*R, yo, Math.sin(ang)*R], col:C.DSW[i], shape:'sphere'}, 0.42); 
        dswObjects.push({ang, R, baseY:yo});
    }

    // Animation Loop
    const clock = new THREE.Clock();
    function loop() {
        requestAnimationFrame(loop); 
        const t = clock.getElapsedTime(); 
        controls.update();
        
        const cog = nodeMap['COG']; 
        if(cog) {
            cog.mesh.rotation.y = t * 0.28; 
            cog.mesh.rotation.x = t * 0.09; 
            cogLight.position.copy(cog.mesh.position); 
            cogLight.intensity = 13 + Math.sin(t * 1.8) * 5; 
            cog.glow.material.opacity = 0.04 + Math.sin(t * 1.2) * 0.02;
        }
        
        nodeMeshes.slice(5).forEach((m,i) => {
            const o = dswObjects[i];
            const na = o.ang + t * (0.05 + i * 0.004);
            m.position.x = Math.cos(na) * o.R; 
            m.position.z = Math.sin(na) * o.R;
            m.position.y = o.baseY + Math.sin(t * 0.9 + i * 0.6) * 0.3; 
            m.rotation.y = t * 0.9;
        });

        composer.render();
    }
    loop();

    window.addEventListener('resize', () => {
        W = window.innerWidth; 
        H = window.innerHeight;
        camera.aspect = W / H; 
        camera.updateProjectionMatrix();
        renderer.setSize(W, H); 
        composer.setSize(W, H); 
        bloom.resolution.set(W, H);
    });
}
