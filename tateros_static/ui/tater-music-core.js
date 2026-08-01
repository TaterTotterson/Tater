//#region node_modules/@vue/shared/dist/shared.esm-bundler.js
// @__NO_SIDE_EFFECTS__
function e(e) {
	let t = /* @__PURE__ */ Object.create(null);
	for (let n of e.split(",")) t[n] = 1;
	return (e) => e in t;
}
var t = {}, n = [], r = () => {}, i = () => !1, a = (e) => e.charCodeAt(0) === 111 && e.charCodeAt(1) === 110 && (e.charCodeAt(2) > 122 || e.charCodeAt(2) < 97), o = (e) => e.startsWith("onUpdate:"), s = Object.assign, c = (e, t) => {
	let n = e.indexOf(t);
	n > -1 && e.splice(n, 1);
}, l = Object.prototype.hasOwnProperty, u = (e, t) => l.call(e, t), d = Array.isArray, f = (e) => x(e) === "[object Map]", p = (e) => x(e) === "[object Set]", m = (e) => x(e) === "[object Date]", h = (e) => typeof e == "function", g = (e) => typeof e == "string", _ = (e) => typeof e == "symbol", v = (e) => typeof e == "object" && !!e, y = (e) => (v(e) || h(e)) && h(e.then) && h(e.catch), b = Object.prototype.toString, x = (e) => b.call(e), S = (e) => x(e).slice(8, -1), C = (e) => x(e) === "[object Object]", w = (e) => g(e) && e !== "NaN" && e[0] !== "-" && "" + parseInt(e, 10) === e, ee = /* @__PURE__ */ e(",key,ref,ref_for,ref_key,onVnodeBeforeMount,onVnodeMounted,onVnodeBeforeUpdate,onVnodeUpdated,onVnodeBeforeUnmount,onVnodeUnmounted"), te = (e) => {
	let t = /* @__PURE__ */ Object.create(null);
	return ((n) => t[n] || (t[n] = e(n)));
}, ne = /-\w/g, T = te((e) => e.replace(ne, (e) => e.slice(1).toUpperCase())), re = /\B([A-Z])/g, E = te((e) => e.replace(re, "-$1").toLowerCase()), ie = te((e) => e.charAt(0).toUpperCase() + e.slice(1)), ae = te((e) => e ? `on${ie(e)}` : ""), D = (e, t) => !Object.is(e, t), oe = (e, ...t) => {
	for (let n = 0; n < e.length; n++) e[n](...t);
}, O = (e, t, n, r = !1) => {
	Object.defineProperty(e, t, {
		configurable: !0,
		enumerable: !1,
		writable: r,
		value: n
	});
}, se = (e) => {
	let t = parseFloat(e);
	return isNaN(t) ? e : t;
}, ce, le = () => ce ||= typeof globalThis < "u" ? globalThis : typeof self < "u" ? self : typeof window < "u" ? window : typeof global < "u" ? global : {};
function ue(e) {
	if (d(e)) {
		let t = {};
		for (let n = 0; n < e.length; n++) {
			let r = e[n], i = g(r) ? me(r) : ue(r);
			if (i) for (let e in i) t[e] = i[e];
		}
		return t;
	}
	if (g(e) || v(e)) return e;
}
var de = /;(?![^(]*\))/g, fe = /:([^]+)/, pe = /\/\*[^]*?\*\//g;
function me(e) {
	let t = {};
	return e.replace(pe, "").split(de).forEach((e) => {
		if (e) {
			let n = e.split(fe);
			n.length > 1 && (t[n[0].trim()] = n[1].trim());
		}
	}), t;
}
function k(e) {
	let t = "";
	if (g(e)) t = e;
	else if (d(e)) for (let n = 0; n < e.length; n++) {
		let r = k(e[n]);
		r && (t += r + " ");
	}
	else if (v(e)) for (let n in e) e[n] && (t += n + " ");
	return t.trim();
}
var he = "itemscope,allowfullscreen,formnovalidate,ismap,nomodule,novalidate,readonly", ge = /* @__PURE__ */ e(he);
he + "";
function _e(e) {
	return !!e || e === "";
}
function ve(e, t) {
	if (e.length !== t.length) return !1;
	let n = !0;
	for (let r = 0; n && r < e.length; r++) n = ye(e[r], t[r]);
	return n;
}
function ye(e, t) {
	if (e === t) return !0;
	let n = m(e), r = m(t);
	if (n || r) return n && r ? e.getTime() === t.getTime() : !1;
	if (n = _(e), r = _(t), n || r) return e === t;
	if (n = d(e), r = d(t), n || r) return n && r ? ve(e, t) : !1;
	if (n = v(e), r = v(t), n || r) {
		if (!n || !r || Object.keys(e).length !== Object.keys(t).length) return !1;
		for (let n in e) {
			let r = e.hasOwnProperty(n), i = t.hasOwnProperty(n);
			if (r && !i || !r && i || !ye(e[n], t[n])) return !1;
		}
	}
	return String(e) === String(t);
}
var be = (e) => !!(e && e.__v_isRef === !0), A = (e) => g(e) ? e : e == null ? "" : d(e) || v(e) && (e.toString === b || !h(e.toString)) ? be(e) ? A(e.value) : JSON.stringify(e, xe, 2) : String(e), xe = (e, t) => be(t) ? xe(e, t.value) : f(t) ? { [`Map(${t.size})`]: [...t.entries()].reduce((e, [t, n], r) => (e[Se(t, r) + " =>"] = n, e), {}) } : p(t) ? { [`Set(${t.size})`]: [...t.values()].map((e) => Se(e)) } : _(t) ? Se(t) : v(t) && !d(t) && !C(t) ? String(t) : t, Se = (e, t = "") => _(e) ? `Symbol(${e.description ?? t})` : e, j, Ce = class {
	constructor(e = !1) {
		this.detached = e, this._active = !0, this._on = 0, this.effects = [], this.cleanups = [], this._isPaused = !1, this._warnOnRun = !0, this.__v_skip = !0, !e && j && (j.active ? (this.parent = j, this.index = (j.scopes || (j.scopes = [])).push(this) - 1) : (this._active = !1, this._warnOnRun = !1));
	}
	get active() {
		return this._active;
	}
	pause() {
		if (this._active) {
			this._isPaused = !0;
			let e, t;
			if (this.scopes) {
				let n = this.scopes.slice();
				for (e = 0, t = n.length; e < t; e++) n[e].pause();
			}
			for (e = 0, t = this.effects.length; e < t; e++) this.effects[e].pause();
		}
	}
	resume() {
		if (this._active && this._isPaused) {
			this._isPaused = !1;
			let e, t;
			if (this.scopes) {
				let n = this.scopes.slice();
				for (e = 0, t = n.length; e < t; e++) n[e].resume();
			}
			let n = this.effects.slice();
			for (e = 0, t = n.length; e < t; e++) n[e].resume();
		}
	}
	run(e) {
		if (this._active) {
			let t = j;
			try {
				return j = this, e();
			} finally {
				j = t;
			}
		}
	}
	on() {
		++this._on === 1 && (this.prevScope = j, j = this);
	}
	off() {
		if (this._on > 0 && --this._on === 0) {
			if (j === this) j = this.prevScope;
			else {
				let e = j;
				for (; e;) {
					if (e.prevScope === this) {
						e.prevScope = this.prevScope;
						break;
					}
					e = e.prevScope;
				}
			}
			this.prevScope = void 0;
		}
	}
	stop(e) {
		if (this._active) {
			this._active = !1;
			let t, n;
			for (t = 0, n = this.effects.length; t < n; t++) this.effects[t].stop();
			for (this.effects.length = 0, t = 0, n = this.cleanups.length; t < n; t++) this.cleanups[t]();
			if (this.cleanups.length = 0, this.scopes) {
				let e = this.scopes.slice();
				for (t = 0, n = e.length; t < n; t++) e[t].stop(!0);
				this.scopes.length = 0;
			}
			if (!this.detached && this.parent && !e) {
				let e = this.parent.scopes.pop();
				e && e !== this && (this.parent.scopes[this.index] = e, e.index = this.index);
			}
			this.parent = void 0;
		}
	}
};
function we() {
	return j;
}
var M, Te = /* @__PURE__ */ new WeakSet(), Ee = class {
	constructor(e) {
		this.fn = e, this.deps = void 0, this.depsTail = void 0, this.flags = 5, this.next = void 0, this.cleanup = void 0, this.scheduler = void 0, j && (j.active ? j.effects.push(this) : this.flags &= -2);
	}
	pause() {
		this.flags |= 64;
	}
	resume() {
		this.flags & 64 && (this.flags &= -65, Te.has(this) && (Te.delete(this), this.trigger()));
	}
	notify() {
		this.flags & 2 && !(this.flags & 32) || this.flags & 8 || Ae(this);
	}
	run() {
		if (!(this.flags & 1)) return this.fn();
		this.flags |= 2, He(this), Ne(this);
		let e = M, t = N;
		M = this, N = !0;
		try {
			return this.fn();
		} finally {
			Pe(this), M = e, N = t, this.flags &= -3;
		}
	}
	stop() {
		if (this.flags & 1) {
			for (let e = this.deps; e; e = e.nextDep) Le(e);
			this.deps = this.depsTail = void 0, He(this), this.onStop && this.onStop(), this.flags &= -2;
		}
	}
	trigger() {
		this.flags & 64 ? Te.add(this) : this.scheduler ? this.scheduler() : this.runIfDirty();
	}
	runIfDirty() {
		Fe(this) && this.run();
	}
	get dirty() {
		return Fe(this);
	}
}, De = 0, Oe, ke;
function Ae(e, t = !1) {
	if (e.flags |= 8, t) {
		e.next = ke, ke = e;
		return;
	}
	e.next = Oe, Oe = e;
}
function je() {
	De++;
}
function Me() {
	if (--De > 0) return;
	if (ke) {
		let e = ke;
		for (ke = void 0; e;) {
			let t = e.next;
			e.next = void 0, e.flags &= -9, e = t;
		}
	}
	let e;
	for (; Oe;) {
		let t = Oe;
		for (Oe = void 0; t;) {
			let n = t.next;
			if (t.next = void 0, t.flags &= -9, t.flags & 1) try {
				t.trigger();
			} catch (t) {
				e ||= t;
			}
			t = n;
		}
	}
	if (e) throw e;
}
function Ne(e) {
	for (let t = e.deps; t; t = t.nextDep) t.version = -1, t.prevActiveLink = t.dep.activeLink, t.dep.activeLink = t;
}
function Pe(e) {
	let t, n = e.depsTail, r = n;
	for (; r;) {
		let e = r.prevDep;
		r.version === -1 ? (r === n && (n = e), Le(r), Re(r)) : t = r, r.dep.activeLink = r.prevActiveLink, r.prevActiveLink = void 0, r = e;
	}
	e.deps = t, e.depsTail = n;
}
function Fe(e) {
	for (let t = e.deps; t; t = t.nextDep) if (t.dep.version !== t.version || t.dep.computed && (Ie(t.dep.computed) || t.dep.version !== t.version)) return !0;
	return !!e._dirty;
}
function Ie(e) {
	if (e.flags & 4 && !(e.flags & 16) || (e.flags &= -17, e.globalVersion === Ue) || (e.globalVersion = Ue, !e.isSSR && e.flags & 128 && (!e.deps && !e._dirty || !Fe(e)))) return;
	e.flags |= 2;
	let t = e.dep, n = M, r = N;
	M = e, N = !0;
	try {
		Ne(e);
		let n = e.fn(e._value);
		(t.version === 0 || D(n, e._value)) && (e.flags |= 128, e._value = n, t.version++);
	} catch (e) {
		throw t.version++, e;
	} finally {
		M = n, N = r, Pe(e), e.flags &= -3;
	}
}
function Le(e, t = !1) {
	let { dep: n, prevSub: r, nextSub: i } = e;
	if (r && (r.nextSub = i, e.prevSub = void 0), i && (i.prevSub = r, e.nextSub = void 0), n.subs === e && (n.subs = r, !r && n.computed)) {
		n.computed.flags &= -5;
		for (let e = n.computed.deps; e; e = e.nextDep) Le(e, !0);
	}
	!t && !--n.sc && n.map && n.map.delete(n.key);
}
function Re(e) {
	let { prevDep: t, nextDep: n } = e;
	t && (t.nextDep = n, e.prevDep = void 0), n && (n.prevDep = t, e.nextDep = void 0);
}
var N = !0, ze = [];
function Be() {
	ze.push(N), N = !1;
}
function Ve() {
	let e = ze.pop();
	N = e === void 0 || e;
}
function He(e) {
	let { cleanup: t } = e;
	if (e.cleanup = void 0, t) {
		let e = M;
		M = void 0;
		try {
			t();
		} finally {
			M = e;
		}
	}
}
var Ue = 0, We = class {
	constructor(e, t) {
		this.sub = e, this.dep = t, this.version = t.version, this.nextDep = this.prevDep = this.nextSub = this.prevSub = this.prevActiveLink = void 0;
	}
}, Ge = class {
	constructor(e) {
		this.computed = e, this.version = 0, this.activeLink = void 0, this.subs = void 0, this.map = void 0, this.key = void 0, this.sc = 0, this.__v_skip = !0;
	}
	track(e) {
		if (!M || !N || M === this.computed) return;
		let t = this.activeLink;
		if (t === void 0 || t.sub !== M) t = this.activeLink = new We(M, this), M.deps ? (t.prevDep = M.depsTail, M.depsTail.nextDep = t, M.depsTail = t) : M.deps = M.depsTail = t, Ke(t);
		else if (t.version === -1 && (t.version = this.version, t.nextDep)) {
			let e = t.nextDep;
			e.prevDep = t.prevDep, t.prevDep && (t.prevDep.nextDep = e), t.prevDep = M.depsTail, t.nextDep = void 0, M.depsTail.nextDep = t, M.depsTail = t, M.deps === t && (M.deps = e);
		}
		return t;
	}
	trigger(e) {
		this.version++, Ue++, this.notify(e);
	}
	notify(e) {
		je();
		try {
			for (let e = this.subs; e; e = e.prevSub) e.sub.notify() && e.sub.dep.notify();
		} finally {
			Me();
		}
	}
};
function Ke(e) {
	if (e.dep.sc++, e.sub.flags & 4) {
		let t = e.dep.computed;
		if (t && !e.dep.subs) {
			t.flags |= 20;
			for (let e = t.deps; e; e = e.nextDep) Ke(e);
		}
		let n = e.dep.subs;
		n !== e && (e.prevSub = n, n && (n.nextSub = e)), e.dep.subs = e;
	}
}
var qe = /* @__PURE__ */ new WeakMap(), Je = /* @__PURE__ */ Symbol(""), Ye = /* @__PURE__ */ Symbol(""), Xe = /* @__PURE__ */ Symbol("");
function P(e, t, n) {
	if (N && M) {
		let t = qe.get(e);
		t || qe.set(e, t = /* @__PURE__ */ new Map());
		let r = t.get(n);
		r || (t.set(n, r = new Ge()), r.map = t, r.key = n), r.track();
	}
}
function Ze(e, t, n, r, i, a) {
	let o = qe.get(e);
	if (!o) {
		Ue++;
		return;
	}
	let s = (e) => {
		e && e.trigger();
	};
	if (je(), t === "clear") o.forEach(s);
	else {
		let i = d(e), a = i && w(n);
		if (i && n === "length") {
			let e = Number(r);
			o.forEach((t, n) => {
				(n === "length" || n === Xe || !_(n) && n >= e) && s(t);
			});
		} else switch ((n !== void 0 || o.has(void 0)) && s(o.get(n)), a && s(o.get(Xe)), t) {
			case "add":
				i ? a && s(o.get("length")) : (s(o.get(Je)), f(e) && s(o.get(Ye)));
				break;
			case "delete":
				i || (s(o.get(Je)), f(e) && s(o.get(Ye)));
				break;
			case "set": f(e) && s(o.get(Je));
		}
	}
	Me();
}
function Qe(e) {
	let t = /* @__PURE__ */ I(e);
	return t === e ? t : (P(t, "iterate", Xe), /* @__PURE__ */ F(e) ? t : t.map(L));
}
function $e(e) {
	return P(e = /* @__PURE__ */ I(e), "iterate", Xe), e;
}
function et(e, t) {
	return /* @__PURE__ */ It(e) ? zt(/* @__PURE__ */ Ft(e) ? L(t) : t) : L(t);
}
var tt = {
	__proto__: null,
	[Symbol.iterator]() {
		return nt(this, Symbol.iterator, (e) => et(this, e));
	},
	concat(...e) {
		return Qe(this).concat(...e.map((e) => d(e) ? Qe(e) : e));
	},
	entries() {
		return nt(this, "entries", (e) => (e[1] = et(this, e[1]), e));
	},
	every(e, t) {
		return it(this, "every", e, t, void 0, arguments);
	},
	filter(e, t) {
		return it(this, "filter", e, t, (e) => e.map((e) => et(this, e)), arguments);
	},
	find(e, t) {
		return it(this, "find", e, t, (e) => et(this, e), arguments);
	},
	findIndex(e, t) {
		return it(this, "findIndex", e, t, void 0, arguments);
	},
	findLast(e, t) {
		return it(this, "findLast", e, t, (e) => et(this, e), arguments);
	},
	findLastIndex(e, t) {
		return it(this, "findLastIndex", e, t, void 0, arguments);
	},
	forEach(e, t) {
		return it(this, "forEach", e, t, void 0, arguments);
	},
	includes(...e) {
		return ot(this, "includes", e);
	},
	indexOf(...e) {
		return ot(this, "indexOf", e);
	},
	join(e) {
		return Qe(this).join(e);
	},
	lastIndexOf(...e) {
		return ot(this, "lastIndexOf", e);
	},
	map(e, t) {
		return it(this, "map", e, t, void 0, arguments);
	},
	pop() {
		return st(this, "pop");
	},
	push(...e) {
		return st(this, "push", e);
	},
	reduce(e, ...t) {
		return at(this, "reduce", e, t);
	},
	reduceRight(e, ...t) {
		return at(this, "reduceRight", e, t);
	},
	shift() {
		return st(this, "shift");
	},
	some(e, t) {
		return it(this, "some", e, t, void 0, arguments);
	},
	splice(...e) {
		return st(this, "splice", e);
	},
	toReversed() {
		return Qe(this).toReversed();
	},
	toSorted(e) {
		return Qe(this).toSorted(e);
	},
	toSpliced(...e) {
		return Qe(this).toSpliced(...e);
	},
	unshift(...e) {
		return st(this, "unshift", e);
	},
	values() {
		return nt(this, "values", (e) => et(this, e));
	}
};
function nt(e, t, n) {
	let r = $e(e), i = r[t]();
	return r !== e && !/* @__PURE__ */ F(e) && (i._next = i.next, i.next = () => {
		let e = i._next();
		return e.done || (e.value = n(e.value)), e;
	}), i;
}
var rt = Array.prototype;
function it(e, t, n, r, i, a) {
	let o = $e(e), s = o !== e && !/* @__PURE__ */ F(e), c = o[t];
	if (c !== rt[t]) {
		let t = c.apply(e, a);
		return s ? L(t) : t;
	}
	let l = n;
	o !== e && (s ? l = function(t, r) {
		return n.call(this, et(e, t), r, e);
	} : n.length > 2 && (l = function(t, r) {
		return n.call(this, t, r, e);
	}));
	let u = c.call(o, l, r);
	return s && i ? i(u) : u;
}
function at(e, t, n, r) {
	let i = $e(e), a = i !== e && !/* @__PURE__ */ F(e), o = n, s = !1;
	i !== e && (a ? (s = r.length === 0, o = function(t, r, i) {
		return s && (s = !1, t = et(e, t)), n.call(this, t, et(e, r), i, e);
	}) : n.length > 3 && (o = function(t, r, i) {
		return n.call(this, t, r, i, e);
	}));
	let c = i[t](o, ...r);
	return s ? et(e, c) : c;
}
function ot(e, t, n) {
	let r = /* @__PURE__ */ I(e);
	P(r, "iterate", Xe);
	let i = r[t](...n);
	return (i === -1 || i === !1) && /* @__PURE__ */ Lt(n[0]) ? (n[0] = /* @__PURE__ */ I(n[0]), r[t](...n)) : i;
}
function st(e, t, n = []) {
	Be(), je();
	let r = (/* @__PURE__ */ I(e))[t].apply(e, n);
	return Me(), Ve(), r;
}
var ct = /* @__PURE__ */ e("__proto__,__v_isRef,__isVue"), lt = new Set(/* @__PURE__ */ Object.getOwnPropertyNames(Symbol).filter((e) => e !== "arguments" && e !== "caller").map((e) => Symbol[e]).filter(_));
function ut(e) {
	_(e) || (e = String(e));
	let t = /* @__PURE__ */ I(this);
	return P(t, "has", e), t.hasOwnProperty(e);
}
var dt = class {
	constructor(e = !1, t = !1) {
		this._isReadonly = e, this._isShallow = t;
	}
	get(e, t, n) {
		if (t === "__v_skip") return e.__v_skip;
		let r = this._isReadonly, i = this._isShallow;
		if (t === "__v_isReactive") return !r;
		if (t === "__v_isReadonly") return r;
		if (t === "__v_isShallow") return i;
		if (t === "__v_raw") return n === (r ? i ? kt : Ot : i ? Dt : Et).get(e) || Object.getPrototypeOf(e) === Object.getPrototypeOf(n) ? e : void 0;
		let a = d(e);
		if (!r) {
			let e;
			if (a && (e = tt[t])) return e;
			if (t === "hasOwnProperty") return ut;
		}
		let o = Reflect.get(e, t, /* @__PURE__ */ R(e) ? e : n);
		if ((_(t) ? lt.has(t) : ct(t)) || (r || P(e, "get", t), i)) return o;
		if (/* @__PURE__ */ R(o)) {
			let e = a && w(t) ? o : o.value;
			return r && v(e) ? /* @__PURE__ */ Nt(e) : e;
		}
		return v(o) ? r ? /* @__PURE__ */ Nt(o) : /* @__PURE__ */ jt(o) : o;
	}
}, ft = class extends dt {
	constructor(e = !1) {
		super(!1, e);
	}
	set(e, t, n, r) {
		let i = e[t], a = d(e) && w(t);
		if (!this._isShallow) {
			let e = /* @__PURE__ */ It(i);
			if (!/* @__PURE__ */ F(n) && !/* @__PURE__ */ It(n) && (i = /* @__PURE__ */ I(i), n = /* @__PURE__ */ I(n)), !a && /* @__PURE__ */ R(i) && !/* @__PURE__ */ R(n)) return e || (i.value = n), !0;
		}
		let o = a ? Number(t) < e.length : u(e, t), s = Reflect.set(e, t, n, /* @__PURE__ */ R(e) ? e : r);
		return e === /* @__PURE__ */ I(r) && s && (o ? D(n, i) && Ze(e, "set", t, n, i) : Ze(e, "add", t, n)), s;
	}
	deleteProperty(e, t) {
		let n = u(e, t), r = e[t], i = Reflect.deleteProperty(e, t);
		return i && n && Ze(e, "delete", t, void 0, r), i;
	}
	has(e, t) {
		let n = Reflect.has(e, t);
		return (!_(t) || !lt.has(t)) && P(e, "has", t), n;
	}
	ownKeys(e) {
		return P(e, "iterate", d(e) ? "length" : Je), Reflect.ownKeys(e);
	}
}, pt = class extends dt {
	constructor(e = !1) {
		super(!0, e);
	}
	set(e, t) {
		return !0;
	}
	deleteProperty(e, t) {
		return !0;
	}
}, mt = /* @__PURE__ */ new ft(), ht = /* @__PURE__ */ new pt(), gt = /* @__PURE__ */ new ft(!0), _t = (e) => e, vt = (e) => Reflect.getPrototypeOf(e);
function yt(e, t, n) {
	return function(...r) {
		let i = this.__v_raw, a = /* @__PURE__ */ I(i), o = f(a), c = e === "entries" || e === Symbol.iterator && o, l = e === "keys" && o, u = i[e](...r), d = n ? _t : t ? zt : L;
		return !t && P(a, "iterate", l ? Ye : Je), s(Object.create(u), { next() {
			let { value: e, done: t } = u.next();
			return t ? {
				value: e,
				done: t
			} : {
				value: c ? [d(e[0]), d(e[1])] : d(e),
				done: t
			};
		} });
	};
}
function bt(e) {
	return function(...t) {
		return e === "delete" ? !1 : e === "clear" ? void 0 : this;
	};
}
function xt(e, t) {
	let n = {
		get(n) {
			let r = this.__v_raw, i = /* @__PURE__ */ I(r), a = /* @__PURE__ */ I(n);
			e || (D(n, a) && P(i, "get", n), P(i, "get", a));
			let { has: o } = vt(i), s = t ? _t : e ? zt : L;
			if (o.call(i, n)) return s(r.get(n));
			if (o.call(i, a)) return s(r.get(a));
			r !== i && r.get(n);
		},
		get size() {
			let t = this.__v_raw;
			return !e && P(/* @__PURE__ */ I(t), "iterate", Je), t.size;
		},
		has(t) {
			let n = this.__v_raw, r = /* @__PURE__ */ I(n), i = /* @__PURE__ */ I(t);
			return e || (D(t, i) && P(r, "has", t), P(r, "has", i)), t === i ? n.has(t) : n.has(t) || n.has(i);
		},
		forEach(n, r) {
			let i = this, a = i.__v_raw, o = /* @__PURE__ */ I(a), s = t ? _t : e ? zt : L;
			return !e && P(o, "iterate", Je), a.forEach((e, t) => n.call(r, s(e), s(t), i));
		}
	};
	return s(n, e ? {
		add: bt("add"),
		set: bt("set"),
		delete: bt("delete"),
		clear: bt("clear")
	} : {
		add(e) {
			let n = /* @__PURE__ */ I(this), r = vt(n), i = /* @__PURE__ */ I(e), a = !t && !/* @__PURE__ */ F(e) && !/* @__PURE__ */ It(e) ? i : e;
			return r.has.call(n, a) || D(e, a) && r.has.call(n, e) || D(i, a) && r.has.call(n, i) || (n.add(a), Ze(n, "add", a, a)), this;
		},
		set(e, n) {
			!t && !/* @__PURE__ */ F(n) && !/* @__PURE__ */ It(n) && (n = /* @__PURE__ */ I(n));
			let r = /* @__PURE__ */ I(this), { has: i, get: a } = vt(r), o = i.call(r, e);
			o ||= (e = /* @__PURE__ */ I(e), i.call(r, e));
			let s = a.call(r, e);
			return r.set(e, n), o ? D(n, s) && Ze(r, "set", e, n, s) : Ze(r, "add", e, n), this;
		},
		delete(e) {
			let t = /* @__PURE__ */ I(this), { has: n, get: r } = vt(t), i = n.call(t, e);
			i ||= (e = /* @__PURE__ */ I(e), n.call(t, e));
			let a = r ? r.call(t, e) : void 0, o = t.delete(e);
			return i && Ze(t, "delete", e, void 0, a), o;
		},
		clear() {
			let e = /* @__PURE__ */ I(this), t = e.size !== 0, n = e.clear();
			return t && Ze(e, "clear", void 0, void 0, void 0), n;
		}
	}), [
		"keys",
		"values",
		"entries",
		Symbol.iterator
	].forEach((r) => {
		n[r] = yt(r, e, t);
	}), n;
}
function St(e, t) {
	let n = xt(e, t);
	return (t, r, i) => r === "__v_isReactive" ? !e : r === "__v_isReadonly" ? e : r === "__v_raw" ? t : Reflect.get(u(n, r) && r in t ? n : t, r, i);
}
var Ct = { get: /* @__PURE__ */ St(!1, !1) }, wt = { get: /* @__PURE__ */ St(!1, !0) }, Tt = { get: /* @__PURE__ */ St(!0, !1) }, Et = /* @__PURE__ */ new WeakMap(), Dt = /* @__PURE__ */ new WeakMap(), Ot = /* @__PURE__ */ new WeakMap(), kt = /* @__PURE__ */ new WeakMap();
function At(e) {
	switch (e) {
		case "Object":
		case "Array": return 1;
		case "Map":
		case "Set":
		case "WeakMap":
		case "WeakSet": return 2;
		default: return 0;
	}
}
// @__NO_SIDE_EFFECTS__
function jt(e) {
	return /* @__PURE__ */ It(e) ? e : Pt(e, !1, mt, Ct, Et);
}
// @__NO_SIDE_EFFECTS__
function Mt(e) {
	return Pt(e, !1, gt, wt, Dt);
}
// @__NO_SIDE_EFFECTS__
function Nt(e) {
	return Pt(e, !0, ht, Tt, Ot);
}
function Pt(e, t, n, r, i) {
	if (!v(e) || e.__v_raw && !(t && e.__v_isReactive) || e.__v_skip || !Object.isExtensible(e)) return e;
	let a = i.get(e);
	if (a) return a;
	let o = At(S(e));
	if (o === 0) return e;
	let s = new Proxy(e, o === 2 ? r : n);
	return i.set(e, s), s;
}
// @__NO_SIDE_EFFECTS__
function Ft(e) {
	return /* @__PURE__ */ It(e) ? /* @__PURE__ */ Ft(e.__v_raw) : !!(e && e.__v_isReactive);
}
// @__NO_SIDE_EFFECTS__
function It(e) {
	return !!(e && e.__v_isReadonly);
}
// @__NO_SIDE_EFFECTS__
function F(e) {
	return !!(e && e.__v_isShallow);
}
// @__NO_SIDE_EFFECTS__
function Lt(e) {
	return e ? !!e.__v_raw : !1;
}
// @__NO_SIDE_EFFECTS__
function I(e) {
	let t = e && e.__v_raw;
	return t ? /* @__PURE__ */ I(t) : e;
}
function Rt(e) {
	return !u(e, "__v_skip") && Object.isExtensible(e) && O(e, "__v_skip", !0), e;
}
var L = (e) => v(e) ? /* @__PURE__ */ jt(e) : e, zt = (e) => v(e) ? /* @__PURE__ */ Nt(e) : e;
// @__NO_SIDE_EFFECTS__
function R(e) {
	return e ? e.__v_isRef === !0 : !1;
}
// @__NO_SIDE_EFFECTS__
function Bt(e) {
	return Vt(e, !1);
}
function Vt(e, t) {
	return /* @__PURE__ */ R(e) ? e : new Ht(e, t);
}
var Ht = class {
	constructor(e, t) {
		this.dep = new Ge(), this.__v_isRef = !0, this.__v_isShallow = !1, this._rawValue = t ? e : /* @__PURE__ */ I(e), this._value = t ? e : L(e), this.__v_isShallow = t;
	}
	get value() {
		return this.dep.track(), this._value;
	}
	set value(e) {
		let t = this._rawValue, n = this.__v_isShallow || /* @__PURE__ */ F(e) || /* @__PURE__ */ It(e);
		e = n ? e : /* @__PURE__ */ I(e), D(e, t) && (this._rawValue = e, this._value = n ? e : L(e), this.dep.trigger());
	}
};
function Ut(e) {
	return /* @__PURE__ */ R(e) ? e.value : e;
}
var Wt = {
	get: (e, t, n) => t === "__v_raw" ? e : Ut(Reflect.get(e, t, n)),
	set: (e, t, n, r) => {
		let i = e[t];
		return /* @__PURE__ */ R(i) && !/* @__PURE__ */ R(n) ? (i.value = n, !0) : Reflect.set(e, t, n, r);
	}
};
function Gt(e) {
	return /* @__PURE__ */ Ft(e) ? e : new Proxy(e, Wt);
}
var Kt = class {
	constructor(e, t, n) {
		this.fn = e, this.setter = t, this._value = void 0, this.dep = new Ge(this), this.__v_isRef = !0, this.deps = void 0, this.depsTail = void 0, this.flags = 16, this.globalVersion = Ue - 1, this.next = void 0, this.effect = this, this.__v_isReadonly = !t, this.isSSR = n;
	}
	notify() {
		if (this.flags |= 16, !(this.flags & 8) && M !== this) return Ae(this, !0), !0;
	}
	get value() {
		let e = this.dep.track();
		return Ie(this), e && (e.version = this.dep.version), this._value;
	}
	set value(e) {
		this.setter && this.setter(e);
	}
};
// @__NO_SIDE_EFFECTS__
function qt(e, t, n = !1) {
	let r, i;
	return h(e) ? r = e : (r = e.get, i = e.set), new Kt(r, i, n);
}
var Jt = {}, Yt = /* @__PURE__ */ new WeakMap(), Xt = void 0;
function Zt(e, t = !1, n = Xt) {
	if (n) {
		let t = Yt.get(n);
		t || Yt.set(n, t = []), t.push(e);
	}
}
function Qt(e, n, i = t) {
	let { immediate: a, deep: o, once: s, scheduler: l, augmentJob: u, call: f } = i, p = (e) => o ? e : /* @__PURE__ */ F(e) || o === !1 || o === 0 ? $t(e, 1) : $t(e), m, g, _, v, y = !1, b = !1;
	if (/* @__PURE__ */ R(e) ? (g = () => e.value, y = /* @__PURE__ */ F(e)) : /* @__PURE__ */ Ft(e) ? (g = () => p(e), y = !0) : d(e) ? (b = !0, y = e.some((e) => /* @__PURE__ */ Ft(e) || /* @__PURE__ */ F(e)), g = () => e.map((e) => {
		if (/* @__PURE__ */ R(e)) return e.value;
		if (/* @__PURE__ */ Ft(e)) return p(e);
		if (h(e)) return f ? f(e, 2) : e();
	})) : g = h(e) ? n ? f ? () => f(e, 2) : e : () => {
		if (_) {
			Be();
			try {
				_();
			} finally {
				Ve();
			}
		}
		let t = Xt;
		Xt = m;
		try {
			return f ? f(e, 3, [v]) : e(v);
		} finally {
			Xt = t;
		}
	} : r, n && o) {
		let e = g, t = o === !0 ? Infinity : o;
		g = () => $t(e(), t);
	}
	let x = we(), S = () => {
		m.stop(), x && x.active && c(x.effects, m);
	};
	if (s && n) {
		let e = n;
		n = (...t) => {
			let n = e(...t);
			return S(), n;
		};
	}
	let C = b ? Array(e.length).fill(Jt) : Jt, w = (e) => {
		if (!(!(m.flags & 1) || !m.dirty && !e)) if (n) {
			let t = m.run();
			if (e || o || y || (b ? t.some((e, t) => D(e, C[t])) : D(t, C))) {
				_ && _();
				let e = Xt;
				Xt = m;
				try {
					let e = [
						t,
						C === Jt ? void 0 : b && C[0] === Jt ? [] : C,
						v
					];
					C = t, f ? f(n, 3, e) : n(...e);
				} finally {
					Xt = e;
				}
			}
		} else m.run();
	};
	return u && u(w), m = new Ee(g), m.scheduler = l ? () => l(w, !1) : w, v = (e) => Zt(e, !1, m), _ = m.onStop = () => {
		let e = Yt.get(m);
		if (e) {
			if (f) f(e, 4);
			else for (let t of e) t();
			Yt.delete(m);
		}
	}, n ? a ? w(!0) : C = m.run() : l ? l(w.bind(null, !0), !0) : m.run(), S.pause = m.pause.bind(m), S.resume = m.resume.bind(m), S.stop = S, S;
}
function $t(e, t = Infinity, n) {
	if (t <= 0 || !v(e) || e.__v_skip || (n ||= /* @__PURE__ */ new Map(), (n.get(e) || 0) >= t)) return e;
	if (n.set(e, t), t--, /* @__PURE__ */ R(e)) $t(e.value, t, n);
	else if (d(e)) for (let r = 0; r < e.length; r++) $t(e[r], t, n);
	else if (p(e) || f(e)) e.forEach((e) => {
		$t(e, t, n);
	});
	else if (C(e)) {
		for (let r in e) $t(e[r], t, n);
		for (let r of Object.getOwnPropertySymbols(e)) Object.prototype.propertyIsEnumerable.call(e, r) && $t(e[r], t, n);
	}
	return e;
}
//#endregion
//#region node_modules/@vue/runtime-core/dist/runtime-core.esm-bundler.js
function en(e, t, n, r) {
	try {
		return r ? e(...r) : e();
	} catch (e) {
		tn(e, t, n);
	}
}
function z(e, t, n, r) {
	if (h(e)) {
		let i = en(e, t, n, r);
		return i && y(i) && i.catch((e) => {
			tn(e, t, n);
		}), i;
	}
	if (d(e)) {
		let i = [];
		for (let a = 0; a < e.length; a++) i.push(z(e[a], t, n, r));
		return i;
	}
}
function tn(e, n, r, i = !0) {
	let a = n ? n.vnode : null, { errorHandler: o, throwUnhandledErrorInProduction: s } = n && n.appContext.config || t;
	if (n) {
		let t = n.parent, i = n.proxy, a = `https://vuejs.org/error-reference/#runtime-${r}`;
		for (; t;) {
			let n = t.ec;
			if (n) {
				for (let t = 0; t < n.length; t++) if (n[t](e, i, a) === !1) return;
			}
			t = t.parent;
		}
		if (o) {
			Be(), en(o, null, 10, [
				e,
				i,
				a
			]), Ve();
			return;
		}
	}
	nn(e, r, a, i, s);
}
function nn(e, t, n, r = !0, i = !1) {
	if (i) throw e;
	console.error(e);
}
var B = [], rn = -1, an = [], on = null, sn = 0, cn = /* @__PURE__ */ Promise.resolve(), ln = null;
function un(e) {
	let t = ln || cn;
	return e ? t.then(this ? e.bind(this) : e) : t;
}
function dn(e) {
	let t = rn + 1, n = B.length;
	for (; t < n;) {
		let r = t + n >>> 1, i = B[r], a = _n(i);
		a < e || a === e && i.flags & 2 ? t = r + 1 : n = r;
	}
	return t;
}
function fn(e) {
	if (!(e.flags & 1)) {
		let t = _n(e), n = B[B.length - 1];
		!n || !(e.flags & 2) && t >= _n(n) ? B.push(e) : B.splice(dn(t), 0, e), e.flags |= 1, pn();
	}
}
function pn() {
	ln ||= cn.then(vn);
}
function mn(e) {
	d(e) ? an.push(...e) : on && e.id === -1 ? on.splice(sn + 1, 0, e) : e.flags & 1 || (an.push(e), e.flags |= 1), pn();
}
function hn(e, t, n = rn + 1) {
	for (; n < B.length; n++) {
		let t = B[n];
		if (t && t.flags & 2) {
			if (e && t.id !== e.uid) continue;
			B.splice(n, 1), n--, t.flags & 4 && (t.flags &= -2), t(), t.flags & 4 || (t.flags &= -2);
		}
	}
}
function gn(e) {
	if (an.length) {
		let e = [...new Set(an)].sort((e, t) => _n(e) - _n(t));
		if (an.length = 0, on) {
			on.push(...e);
			return;
		}
		for (on = e, sn = 0; sn < on.length; sn++) {
			let e = on[sn];
			e.flags & 4 && (e.flags &= -2), e.flags & 8 || e(), e.flags &= -2;
		}
		on = null, sn = 0;
	}
}
var _n = (e) => e.id == null ? e.flags & 2 ? -1 : Infinity : e.id;
function vn(e) {
	try {
		for (rn = 0; rn < B.length; rn++) {
			let e = B[rn];
			e && !(e.flags & 8) && (e.flags & 4 && (e.flags &= -2), en(e, e.i, e.i ? 15 : 14), e.flags & 4 || (e.flags &= -2));
		}
	} finally {
		for (; rn < B.length; rn++) {
			let e = B[rn];
			e && (e.flags &= -2);
		}
		rn = -1, B.length = 0, gn(e), ln = null, (B.length || an.length) && vn(e);
	}
}
var yn = null, bn = null;
function xn(e) {
	let t = yn;
	return yn = e, bn = e && e.type.__scopeId || null, t;
}
function Sn(e, t = yn, n) {
	if (!t || e._n) return e;
	let r = (...n) => {
		r._d && Ii(-1);
		let i = xn(t), a = Ni.length, o;
		try {
			o = e(...n);
		} finally {
			for (let e = Ni.length; e > a; e--) Pi();
			xn(i), r._d && Ii(1);
		}
		return o;
	};
	return r._n = !0, r._c = !0, r._d = !0, r;
}
function Cn(e, t, n, r) {
	let i = e.dirs, a = t && t.dirs;
	for (let o = 0; o < i.length; o++) {
		let s = i[o];
		a && (s.oldValue = a[o].value);
		let c = s.dir[r];
		c && (Be(), z(c, n, 8, [
			e.el,
			s,
			e,
			t
		]), Ve());
	}
}
function wn(e, t) {
	if (Q) {
		let n = Q.provides, r = Q.parent && Q.parent.provides;
		r === n && (n = Q.provides = Object.create(r)), n[e] = t;
	}
}
function Tn(e, t, n = !1) {
	let r = ea();
	if (r || Vr) {
		let i = Vr ? Vr._context.provides : r ? r.parent == null || r.ce ? r.vnode.appContext && r.vnode.appContext.provides : r.parent.provides : void 0;
		if (i && e in i) return i[e];
		if (arguments.length > 1) return n && h(t) ? t.call(r && r.proxy) : t;
	}
}
var En = /* @__PURE__ */ Symbol.for("v-scx"), Dn = () => Tn(En);
function On(e, t, n) {
	return kn(e, t, n);
}
function kn(e, n, i = t) {
	let { immediate: a, deep: o, flush: c, once: l } = i, u = s({}, i), d = n && a || !n && c !== "post", f;
	if (oa) {
		if (c === "sync") {
			let e = Dn();
			f = e.__watcherHandles ||= [];
		} else if (!d) {
			let e = () => {};
			return e.stop = r, e.resume = r, e.pause = r, e;
		}
	}
	let p = Q;
	u.call = (e, t, n) => z(e, p, t, n);
	let m = !1;
	c === "post" ? u.scheduler = (e) => {
		U(e, p && p.suspense);
	} : c !== "sync" && (m = !0, u.scheduler = (e, t) => {
		t ? e() : fn(e);
	}), u.augmentJob = (e) => {
		n && (e.flags |= 4), m && (e.flags |= 2, p && (e.id = p.uid, e.i = p));
	};
	let h = Qt(e, n, u);
	return oa && (f ? f.push(h) : d && h()), h;
}
function An(e, t, n) {
	let r = this.proxy, i = g(e) ? e.includes(".") ? jn(r, e) : () => r[e] : e.bind(r, r), a;
	h(t) ? a = t : (a = t.handler, n = t);
	let o = ra(this), s = kn(i, a.bind(r), n);
	return o(), s;
}
function jn(e, t) {
	let n = t.split(".");
	return () => {
		let t = e;
		for (let e = 0; e < n.length && t; e++) t = t[n[e]];
		return t;
	};
}
var Mn = /* @__PURE__ */ new WeakMap(), Nn = /* @__PURE__ */ Symbol("_vte"), Pn = (e) => e.__isTeleport, Fn = (e) => e && (e.disabled || e.disabled === ""), In = (e) => e && (e.defer || e.defer === ""), Ln = (e) => typeof SVGElement < "u" && e instanceof SVGElement, Rn = (e) => typeof MathMLElement == "function" && e instanceof MathMLElement, zn = (e, t) => {
	let n = e && e.to;
	return g(n) ? t ? t(n) : null : n;
}, Bn = {
	name: "Teleport",
	__isTeleport: !0,
	process(e, t, n, r, i, a, o, s, c, l) {
		let { mc: u, pc: d, pbc: f, o: { insert: p, querySelector: m, createText: h, createComment: g, parentNode: _ } } = l, v = Fn(t.props), { dynamicChildren: y } = t, b = (e, t, n) => {
			e.shapeFlag & 16 && u(e.children, t, n, i, a, o, s, c);
		}, x = (e = t) => {
			let n = Fn(e.props), r = e.target = zn(e.props, m), a = Gn(r, e, h, p);
			r && (o !== "svg" && Ln(r) ? o = "svg" : o !== "mathml" && Rn(r) && (o = "mathml"), i && i.isCE && (i.ce._teleportTargets || (i.ce._teleportTargets = /* @__PURE__ */ new Set())).add(r), n || (b(e, r, a), Wn(e, !1)));
		}, S = (e) => {
			let t = () => {
				if (Mn.get(e) === t) {
					if (Mn.delete(e), Fn(e.props)) {
						let t = _(e.el) || n;
						b(e, t, e.anchor), Wn(e, !0);
					}
					x(e);
				}
			};
			Mn.set(e, t), U(t, a);
		};
		if (e == null) {
			let e = t.el = h(""), i = t.anchor = h("");
			if (p(e, n, r), p(i, n, r), In(t.props) || a && a.pendingBranch) {
				S(t);
				return;
			}
			v && (b(t, n, i), Wn(t, !0)), x();
		} else {
			t.el = e.el;
			let r = t.anchor = e.anchor, u = Mn.get(e);
			if (u) {
				u.flags |= 8, Mn.delete(e), S(t);
				return;
			}
			t.targetStart = e.targetStart;
			let p = t.target = e.target, h = t.targetAnchor = e.targetAnchor, g = Fn(e.props), _ = g ? n : p, b = g ? r : h;
			if (o === "svg" || Ln(p) ? o = "svg" : (o === "mathml" || Rn(p)) && (o = "mathml"), y ? (f(e.dynamicChildren, y, _, i, a, o, s), Ci(e, t, !0)) : c || d(e, t, _, b, i, a, o, s, !1), v) g ? t.props && e.props && t.props.to !== e.props.to && (t.props.to = e.props.to) : Vn(t, n, r, l, 1);
			else if ((t.props && t.props.to) !== (e.props && e.props.to)) {
				let e = zn(t.props, m);
				e && (t.target = e, Vn(t, e, null, l, 0));
			} else g && Vn(t, p, h, l, 1);
			Wn(t, v);
		}
	},
	remove(e, t, n, { um: r, o: { remove: i } }, a) {
		let { shapeFlag: o, children: s, anchor: c, targetStart: l, targetAnchor: u, target: d, props: f } = e, p = Fn(f), m = a || !p, h = Mn.get(e);
		if (h && (h.flags |= 8, Mn.delete(e)), d && (i(l), i(u)), a && i(c), !h && (p || d) && o & 16) for (let e = 0; e < s.length; e++) {
			let i = s[e];
			r(i, t, n, m, !!i.dynamicChildren);
		}
	},
	move: Vn,
	hydrate: Hn
};
function Vn(e, t, n, { o: { insert: r }, m: i }, a = 2) {
	a === 0 && r(e.targetAnchor, t, n);
	let { el: o, anchor: s, shapeFlag: c, children: l, props: u } = e, d = a === 2;
	if (d && r(o, t, n), !Mn.has(e) && (!d || Fn(u)) && c & 16) for (let e = 0; e < l.length; e++) i(l[e], t, n, 2);
	d && r(s, t, n);
}
function Hn(e, t, n, r, i, a, { o: { nextSibling: o, parentNode: s, querySelector: c, insert: l, createText: u } }, d) {
	function f(e, n) {
		let r = n;
		for (; r;) {
			if (r && r.nodeType === 8) {
				if (r.data === "teleport start anchor") t.targetStart = r;
				else if (r.data === "teleport anchor") {
					t.targetAnchor = r, e._lpa = t.targetAnchor && o(t.targetAnchor);
					break;
				}
			}
			r = o(r);
		}
	}
	function p(e, t) {
		t.anchor = d(o(e), t, s(e), n, r, i, a);
	}
	let m = t.target = zn(t.props, c), h = Fn(t.props);
	if (m) {
		let c = m._lpa || m.firstChild;
		t.shapeFlag & 16 && (h ? (p(e, t), f(m, c), t.targetAnchor || Gn(m, t, u, l, s(e) === m ? e : null)) : (t.anchor = o(e), f(m, c), t.targetAnchor || Gn(m, t, u, l), d(c && o(c), t, m, n, r, i, a))), Wn(t, h);
	} else h && t.shapeFlag & 16 && (p(e, t), t.targetStart = e, t.targetAnchor = o(e));
	return t.anchor && o(t.anchor);
}
var Un = Bn;
function Wn(e, t) {
	let n = e.ctx;
	if (n && n.ut) {
		let r, i;
		for (t ? (r = e.el, i = e.anchor) : (r = e.targetStart, i = e.targetAnchor); r && r !== i;) r.nodeType === 1 && r.setAttribute("data-v-owner", n.uid), r = r.nextSibling;
		n.ut();
	}
}
function Gn(e, t, n, r, i = null) {
	let a = t.targetStart = n(""), o = t.targetAnchor = n("");
	return a[Nn] = o, e && (r(a, e, i), r(o, e, i)), o;
}
var Kn = /* @__PURE__ */ Symbol("_leaveCb");
function qn(e, t) {
	e.shapeFlag & 6 && e.component ? (e.transition = t, qn(e.component.subTree, t)) : e.shapeFlag & 128 ? (e.ssContent.transition = t.clone(e.ssContent), e.ssFallback.transition = t.clone(e.ssFallback)) : e.transition = t;
}
// @__NO_SIDE_EFFECTS__
function Jn(e, t) {
	return h(e) ? /* @__PURE__ */ s({ name: e.name }, t, { setup: e }) : e;
}
function Yn(e) {
	e.ids = [
		e.ids[0] + e.ids[2]++ + "-",
		0,
		0
	];
}
function Xn(e, t) {
	let n;
	return !!((n = Object.getOwnPropertyDescriptor(e, t)) && !n.configurable);
}
var Zn = /* @__PURE__ */ new WeakMap();
function Qn(e, n, r, a, o = !1) {
	if (d(e)) {
		e.forEach((e, t) => Qn(e, n && (d(n) ? n[t] : n), r, a, o));
		return;
	}
	if (er(a) && !o) {
		a.shapeFlag & 512 && a.type.__asyncResolved && a.component.subTree.component && Qn(e, n, r, a.component.subTree);
		return;
	}
	let s = a.shapeFlag & 4 ? ha(a.component) : a.el, l = o ? null : s, { i: f, r: p } = e, m = n && n.r, _ = f.refs === t ? f.refs = {} : f.refs, v = f.setupState, y = /* @__PURE__ */ I(v), b = v === t ? i : (e) => !Xn(_, e) && u(y, e), x = (e, t) => !(t && Xn(_, t));
	if (m != null && m !== p) {
		if ($n(n), g(m)) _[m] = null, b(m) && (v[m] = null);
		else if (/* @__PURE__ */ R(m)) {
			let e = n;
			x(m, e.k) && (m.value = null), e.k && (_[e.k] = null);
		}
	}
	if (h(p)) en(p, f, 12, [l, _]);
	else {
		let t = g(p), n = /* @__PURE__ */ R(p);
		if (t || n) {
			let i = () => {
				if (e.f) {
					let n = t ? b(p) ? v[p] : _[p] : x(p) || !e.k ? p.value : _[e.k];
					if (o) d(n) && c(n, s);
					else if (d(n)) n.includes(s) || n.push(s);
					else if (t) _[p] = [s], b(p) && (v[p] = _[p]);
					else {
						let t = [s];
						x(p, e.k) && (p.value = t), e.k && (_[e.k] = t);
					}
				} else t ? (_[p] = l, b(p) && (v[p] = l)) : n && (x(p, e.k) && (p.value = l), e.k && (_[e.k] = l));
			};
			if (l) {
				let t = () => {
					i(), Zn.delete(e);
				};
				t.id = -1, Zn.set(e, t), U(t, r);
			} else $n(e), i();
		}
	}
}
function $n(e) {
	let t = Zn.get(e);
	t && (t.flags |= 8, Zn.delete(e));
}
le().requestIdleCallback, le().cancelIdleCallback;
var er = (e) => !!e.type.__asyncLoader, tr = (e) => e.type.__isKeepAlive;
function nr(e, t) {
	ir(e, "a", t);
}
function rr(e, t) {
	ir(e, "da", t);
}
function ir(e, t, n = Q) {
	let r = e.__wdc ||= () => {
		let t = n;
		for (; t;) {
			if (t.isDeactivated) return;
			t = t.parent;
		}
		return e();
	};
	if (or(t, r, n), n) {
		let e = n.parent;
		for (; e && e.parent;) tr(e.parent.vnode) && ar(r, t, n, e), e = e.parent;
	}
}
function ar(e, t, n, r) {
	let i = or(t, e, r, !0);
	pr(() => {
		c(r[t], i);
	}, n);
}
function or(e, t, n = Q, r = !1) {
	if (n) {
		let i = n[e] || (n[e] = []), a = t.__weh ||= (...r) => {
			Be();
			let i = ra(n), a = z(t, n, e, r);
			return i(), Ve(), a;
		};
		return r ? i.unshift(a) : i.push(a), a;
	}
}
var sr = (e) => (t, n = Q) => {
	(!oa || e === "sp") && or(e, (...e) => t(...e), n);
}, cr = sr("bm"), lr = sr("m"), ur = sr("bu"), dr = sr("u"), fr = sr("bum"), pr = sr("um"), mr = sr("sp"), hr = sr("rtg"), gr = sr("rtc");
function _r(e, t = Q) {
	or("ec", e, t);
}
var vr = /* @__PURE__ */ Symbol.for("v-ndc");
function V(e, t, n, r) {
	let i, a = n && n[r], o = d(e);
	if (o || g(e)) {
		let n = o && /* @__PURE__ */ Ft(e), r = !1, s = !1;
		n && (r = !/* @__PURE__ */ F(e), s = /* @__PURE__ */ It(e), e = $e(e)), i = Array(e.length);
		for (let n = 0, o = e.length; n < o; n++) i[n] = t(r ? s ? zt(L(e[n])) : L(e[n]) : e[n], n, void 0, a && a[n]);
	} else if (typeof e == "number") {
		i = Array(e);
		for (let n = 0; n < e; n++) i[n] = t(n + 1, n, void 0, a && a[n]);
	} else if (v(e)) if (e[Symbol.iterator]) i = Array.from(e, (e, n) => t(e, n, void 0, a && a[n]));
	else {
		let n = Object.keys(e);
		i = Array(n.length);
		for (let r = 0, o = n.length; r < o; r++) {
			let o = n[r];
			i[r] = t(e[o], o, r, a && a[r]);
		}
	}
	else i = [];
	return n && (n[r] = i), i;
}
var yr = (e) => e ? aa(e) ? ha(e) : yr(e.parent) : null, br = /* @__PURE__ */ s(/* @__PURE__ */ Object.create(null), {
	$: (e) => e,
	$el: (e) => e.vnode.el,
	$data: (e) => e.data,
	$props: (e) => e.props,
	$attrs: (e) => e.attrs,
	$slots: (e) => e.slots,
	$refs: (e) => e.refs,
	$parent: (e) => yr(e.parent),
	$root: (e) => yr(e.root),
	$host: (e) => e.ce,
	$emit: (e) => e.emit,
	$options: (e) => kr(e),
	$forceUpdate: (e) => e.f ||= () => {
		fn(e.update);
	},
	$nextTick: (e) => e.n ||= un.bind(e.proxy),
	$watch: (e) => An.bind(e)
}), xr = (e, n) => e !== t && !e.__isScriptSetup && u(e, n), Sr = {
	get({ _: e }, n) {
		if (n === "__v_skip") return !0;
		let { ctx: r, setupState: i, data: a, props: o, accessCache: s, type: c, appContext: l } = e;
		if (n[0] !== "$") {
			let e = s[n];
			if (e !== void 0) switch (e) {
				case 1: return i[n];
				case 2: return a[n];
				case 4: return r[n];
				case 3: return o[n];
			}
			else if (xr(i, n)) return s[n] = 1, i[n];
			else if (a !== t && u(a, n)) return s[n] = 2, a[n];
			else if (u(o, n)) return s[n] = 3, o[n];
			else if (r !== t && u(r, n)) return s[n] = 4, r[n];
			else wr && (s[n] = 0);
		}
		let d = br[n], f, p;
		if (d) return n === "$attrs" && P(e.attrs, "get", ""), d(e);
		if ((f = c.__cssModules) && (f = f[n])) return f;
		if (r !== t && u(r, n)) return s[n] = 4, r[n];
		if (p = l.config.globalProperties, u(p, n)) return p[n];
	},
	set({ _: e }, n, r) {
		let { data: i, setupState: a, ctx: o } = e;
		return xr(a, n) ? (a[n] = r, !0) : i !== t && u(i, n) ? (i[n] = r, !0) : u(e.props, n) || n[0] === "$" && n.slice(1) in e ? !1 : (o[n] = r, !0);
	},
	has({ _: { data: e, setupState: n, accessCache: r, ctx: i, appContext: a, props: o, type: s } }, c) {
		let l;
		return !!(r[c] || e !== t && c[0] !== "$" && u(e, c) || xr(n, c) || u(o, c) || u(i, c) || u(br, c) || u(a.config.globalProperties, c) || (l = s.__cssModules) && l[c]);
	},
	defineProperty(e, t, n) {
		return n.get == null ? u(n, "value") && this.set(e, t, n.value, null) : e._.accessCache[t] = 0, Reflect.defineProperty(e, t, n);
	}
};
function Cr(e) {
	return d(e) ? e.reduce((e, t) => (e[t] = null, e), {}) : e;
}
var wr = !0;
function Tr(e) {
	let t = kr(e), n = e.proxy, i = e.ctx;
	wr = !1, t.beforeCreate && Dr(t.beforeCreate, e, "bc");
	let { data: a, computed: o, methods: s, watch: c, provide: l, inject: u, created: f, beforeMount: p, mounted: m, beforeUpdate: g, updated: _, activated: y, deactivated: b, beforeDestroy: x, beforeUnmount: S, destroyed: C, unmounted: w, render: ee, renderTracked: te, renderTriggered: ne, errorCaptured: T, serverPrefetch: re, expose: E, inheritAttrs: ie, components: ae, directives: D, filters: oe } = t;
	if (u && Er(u, i, null), s) for (let e in s) {
		let t = s[e];
		h(t) && (i[e] = t.bind(n));
	}
	if (a) {
		let t = a.call(n, n);
		v(t) && (e.data = /* @__PURE__ */ jt(t));
	}
	if (wr = !0, o) for (let e in o) {
		let t = o[e], a = $({
			get: h(t) ? t.bind(n, n) : h(t.get) ? t.get.bind(n, n) : r,
			set: !h(t) && h(t.set) ? t.set.bind(n) : r
		});
		Object.defineProperty(i, e, {
			enumerable: !0,
			configurable: !0,
			get: () => a.value,
			set: (e) => a.value = e
		});
	}
	if (c) for (let e in c) Or(c[e], i, n, e);
	if (l) {
		let e = h(l) ? l.call(n) : l;
		Reflect.ownKeys(e).forEach((t) => {
			wn(t, e[t]);
		});
	}
	f && Dr(f, e, "c");
	function O(e, t) {
		d(t) ? t.forEach((t) => e(t.bind(n))) : t && e(t.bind(n));
	}
	if (O(cr, p), O(lr, m), O(ur, g), O(dr, _), O(nr, y), O(rr, b), O(_r, T), O(gr, te), O(hr, ne), O(fr, S), O(pr, w), O(mr, re), d(E)) if (E.length) {
		let t = e.exposed ||= {};
		E.forEach((e) => {
			Object.defineProperty(t, e, {
				get: () => n[e],
				set: (t) => n[e] = t,
				enumerable: !0
			});
		});
	} else e.exposed ||= {};
	ee && e.render === r && (e.render = ee), ie != null && (e.inheritAttrs = ie), ae && (e.components = ae), D && (e.directives = D), re && Yn(e);
}
function Er(e, t, n = r) {
	d(e) && (e = Pr(e));
	for (let n in e) {
		let r = e[n], i;
		i = v(r) ? "default" in r ? Tn(r.from || n, r.default, !0) : Tn(r.from || n) : Tn(r), /* @__PURE__ */ R(i) ? Object.defineProperty(t, n, {
			enumerable: !0,
			configurable: !0,
			get: () => i.value,
			set: (e) => i.value = e
		}) : t[n] = i;
	}
}
function Dr(e, t, n) {
	z(d(e) ? e.map((e) => e.bind(t.proxy)) : e.bind(t.proxy), t, n);
}
function Or(e, t, n, r) {
	let i = r.includes(".") ? jn(n, r) : () => n[r];
	if (g(e)) {
		let n = t[e];
		h(n) && On(i, n);
	} else if (h(e)) On(i, e.bind(n));
	else if (v(e)) if (d(e)) e.forEach((e) => Or(e, t, n, r));
	else {
		let r = h(e.handler) ? e.handler.bind(n) : t[e.handler];
		h(r) && On(i, r, e);
	}
}
function kr(e) {
	let t = e.type, { mixins: n, extends: r } = t, { mixins: i, optionsCache: a, config: { optionMergeStrategies: o } } = e.appContext, s = a.get(t), c;
	return s ? c = s : !i.length && !n && !r ? c = t : (c = {}, i.length && i.forEach((e) => Ar(c, e, o, !0)), Ar(c, t, o)), v(t) && a.set(t, c), c;
}
function Ar(e, t, n, r = !1) {
	let { mixins: i, extends: a } = t;
	a && Ar(e, a, n, !0), i && i.forEach((t) => Ar(e, t, n, !0));
	for (let i in t) if (!(r && i === "expose")) {
		let r = jr[i] || n && n[i];
		e[i] = r ? r(e[i], t[i]) : t[i];
	}
	return e;
}
var jr = {
	data: Mr,
	props: Ir,
	emits: Ir,
	methods: Fr,
	computed: Fr,
	beforeCreate: H,
	created: H,
	beforeMount: H,
	mounted: H,
	beforeUpdate: H,
	updated: H,
	beforeDestroy: H,
	beforeUnmount: H,
	destroyed: H,
	unmounted: H,
	activated: H,
	deactivated: H,
	errorCaptured: H,
	serverPrefetch: H,
	components: Fr,
	directives: Fr,
	watch: Lr,
	provide: Mr,
	inject: Nr
};
function Mr(e, t) {
	return t ? e ? function() {
		return s(h(e) ? e.call(this, this) : e, h(t) ? t.call(this, this) : t);
	} : t : e;
}
function Nr(e, t) {
	return Fr(Pr(e), Pr(t));
}
function Pr(e) {
	if (d(e)) {
		let t = {};
		for (let n = 0; n < e.length; n++) t[e[n]] = e[n];
		return t;
	}
	return e;
}
function H(e, t) {
	return e ? [...new Set([].concat(e, t))] : t;
}
function Fr(e, t) {
	return e ? s(/* @__PURE__ */ Object.create(null), e, t) : t;
}
function Ir(e, t) {
	return e ? d(e) && d(t) ? [.../* @__PURE__ */ new Set([...e, ...t])] : s(/* @__PURE__ */ Object.create(null), Cr(e), Cr(t ?? {})) : t;
}
function Lr(e, t) {
	if (!e) return t;
	if (!t) return e;
	let n = s(/* @__PURE__ */ Object.create(null), e);
	for (let r in t) n[r] = H(e[r], t[r]);
	return n;
}
function Rr() {
	return {
		app: null,
		config: {
			isNativeTag: i,
			performance: !1,
			globalProperties: {},
			optionMergeStrategies: {},
			errorHandler: void 0,
			warnHandler: void 0,
			compilerOptions: {}
		},
		mixins: [],
		components: {},
		directives: {},
		provides: /* @__PURE__ */ Object.create(null),
		optionsCache: /* @__PURE__ */ new WeakMap(),
		propsCache: /* @__PURE__ */ new WeakMap(),
		emitsCache: /* @__PURE__ */ new WeakMap()
	};
}
var zr = 0;
function Br(e, t) {
	return function(n, r = null) {
		h(n) || (n = s({}, n)), r != null && !v(r) && (r = null);
		let i = Rr(), a = /* @__PURE__ */ new WeakSet(), o = [], c = !1, l = i.app = {
			_uid: zr++,
			_component: n,
			_props: r,
			_container: null,
			_context: i,
			_instance: null,
			version: _a,
			get config() {
				return i.config;
			},
			set config(e) {},
			use(e, ...t) {
				return a.has(e) || (e && h(e.install) ? (a.add(e), e.install(l, ...t)) : h(e) && (a.add(e), e(l, ...t))), l;
			},
			mixin(e) {
				return i.mixins.includes(e) || i.mixins.push(e), l;
			},
			component(e, t) {
				return t ? (i.components[e] = t, l) : i.components[e];
			},
			directive(e, t) {
				return t ? (i.directives[e] = t, l) : i.directives[e];
			},
			mount(a, o, s) {
				if (!c) {
					let u = l._ceVNode || Ui(n, r);
					return u.appContext = i, s === !0 ? s = "svg" : s === !1 && (s = void 0), o && t ? t(u, a) : e(u, a, s), c = !0, l._container = a, a.__vue_app__ = l, ha(u.component);
				}
			},
			onUnmount(e) {
				o.push(e);
			},
			unmount() {
				c && (z(o, l._instance, 16), e(null, l._container), delete l._container.__vue_app__);
			},
			provide(e, t) {
				return i.provides[e] = t, l;
			},
			runWithContext(e) {
				let t = Vr;
				Vr = l;
				try {
					return e();
				} finally {
					Vr = t;
				}
			}
		};
		return l;
	};
}
var Vr = null, Hr = (e, t) => t === "modelValue" || t === "model-value" ? e.modelModifiers : e[`${t}Modifiers`] || e[`${T(t)}Modifiers`] || e[`${E(t)}Modifiers`];
function Ur(e, n, ...r) {
	if (e.isUnmounted) return;
	let i = e.vnode.props || t, a = r, o = n.startsWith("update:"), s = o && Hr(i, n.slice(7));
	s && (s.trim && (a = r.map((e) => g(e) ? e.trim() : e)), s.number && (a = r.map(se)));
	let c, l = i[c = ae(n)] || i[c = ae(T(n))];
	!l && o && (l = i[c = ae(E(n))]), l && z(l, e, 6, a);
	let u = i[c + "Once"];
	if (u) {
		if (!e.emitted) e.emitted = {};
		else if (e.emitted[c]) return;
		e.emitted[c] = !0, z(u, e, 6, a);
	}
}
var Wr = /* @__PURE__ */ new WeakMap();
function Gr(e, t, n = !1) {
	let r = n ? Wr : t.emitsCache, i = r.get(e);
	if (i !== void 0) return i;
	let a = e.emits, o = {}, c = !1;
	if (!h(e)) {
		let r = (e) => {
			let n = Gr(e, t, !0);
			n && (c = !0, s(o, n));
		};
		!n && t.mixins.length && t.mixins.forEach(r), e.extends && r(e.extends), e.mixins && e.mixins.forEach(r);
	}
	return !a && !c ? (v(e) && r.set(e, null), null) : (d(a) ? a.forEach((e) => o[e] = null) : s(o, a), v(e) && r.set(e, o), o);
}
function Kr(e, t) {
	return !e || !a(t) ? !1 : (t = t.slice(2), t = t === "Once" ? t : t.replace(/Once$/, ""), u(e, t[0].toLowerCase() + t.slice(1)) || u(e, E(t)) || u(e, t));
}
function qr(e) {
	let { type: t, vnode: n, proxy: r, withProxy: i, propsOptions: [a], slots: s, attrs: c, emit: l, render: u, renderCache: d, props: f, data: p, setupState: m, ctx: h, inheritAttrs: g } = e, _ = xn(e), v, y;
	try {
		if (n.shapeFlag & 4) {
			let e = i || r, t = e;
			v = X(u.call(t, e, d, f, m, p, h)), y = c;
		} else {
			let e = t;
			v = X(e.length > 1 ? e(f, {
				attrs: c,
				slots: s,
				emit: l
			}) : e(f, null)), y = t.props ? c : Jr(c);
		}
	} catch (t) {
		Ni.length = 0, tn(t, e, 1), v = Ui(ji);
	}
	let b = v;
	if (y && g !== !1) {
		let e = Object.keys(y), { shapeFlag: t } = b;
		e.length && t & 7 && (a && e.some(o) && (y = Yr(y, a)), b = Ki(b, y, !1, !0));
	}
	return n.dirs && (b = Ki(b, null, !1, !0), b.dirs = b.dirs ? b.dirs.concat(n.dirs) : n.dirs), n.transition && qn(b, n.transition), v = b, xn(_), v;
}
var Jr = (e) => {
	let t;
	for (let n in e) (n === "class" || n === "style" || a(n)) && ((t ||= {})[n] = e[n]);
	return t;
}, Yr = (e, t) => {
	let n = {};
	for (let r in e) (!o(r) || !(r.slice(9) in t)) && (n[r] = e[r]);
	return n;
};
function Xr(e, t, n) {
	let { props: r, children: i, component: a } = e, { props: o, children: s, patchFlag: c } = t, l = a.emitsOptions;
	if (t.dirs || t.transition) return !0;
	if (n && c >= 0) {
		if (c & 1024) return !0;
		if (c & 16) return r ? Zr(r, o, l) : !!o;
		if (c & 8) {
			let e = t.dynamicProps;
			for (let t = 0; t < e.length; t++) {
				let n = e[t];
				if (Qr(o, r, n) && !Kr(l, n)) return !0;
			}
		}
	} else return (i || s) && (!s || !s.$stable) ? !0 : r === o ? !1 : r ? !o || Zr(r, o, l) : !!o;
	return !1;
}
function Zr(e, t, n) {
	let r = Object.keys(t);
	if (r.length !== Object.keys(e).length) return !0;
	for (let i = 0; i < r.length; i++) {
		let a = r[i];
		if (Qr(t, e, a) && !Kr(n, a)) return !0;
	}
	return !1;
}
function Qr(e, t, n) {
	let r = e[n], i = t[n];
	return n === "style" && v(r) && v(i) ? !ye(r, i) : r !== i;
}
function $r({ vnode: e, parent: t, suspense: n }, r) {
	for (; t;) {
		let n = t.subTree;
		if (n.suspense && n.suspense.activeBranch === e && (n.suspense.vnode.el = n.el = r, e = n), n === e) (e = t.vnode).el = r, t = t.parent;
		else break;
	}
	n && n.activeBranch === e && (n.vnode.el = r);
}
var ei = {}, ti = () => Object.create(ei), ni = (e) => Object.getPrototypeOf(e) === ei;
function ri(e, t, n, r = !1) {
	let i = {}, a = ti();
	e.propsDefaults = /* @__PURE__ */ Object.create(null), ai(e, t, i, a);
	for (let t in e.propsOptions[0]) t in i || (i[t] = void 0);
	e.props = n ? r ? i : /* @__PURE__ */ Mt(i) : e.type.props ? i : a, e.attrs = a;
}
function ii(e, t, n, r) {
	let { props: i, attrs: a, vnode: { patchFlag: o } } = e, s = /* @__PURE__ */ I(i), [c] = e.propsOptions, l = !1;
	if ((r || o > 0) && !(o & 16)) {
		if (o & 8) {
			let n = e.vnode.dynamicProps;
			for (let r = 0; r < n.length; r++) {
				let o = n[r];
				if (Kr(e.emitsOptions, o)) continue;
				let d = t[o];
				if (c) if (u(a, o)) d !== a[o] && (a[o] = d, l = !0);
				else {
					let t = T(o);
					i[t] = oi(c, s, t, d, e, !1);
				}
				else d !== a[o] && (a[o] = d, l = !0);
			}
		}
	} else {
		ai(e, t, i, a) && (l = !0);
		let r;
		for (let a in s) (!t || !u(t, a) && ((r = E(a)) === a || !u(t, r))) && (c ? n && (n[a] !== void 0 || n[r] !== void 0) && (i[a] = oi(c, s, a, void 0, e, !0)) : delete i[a]);
		if (a !== s) for (let e in a) (!t || !u(t, e)) && (delete a[e], l = !0);
	}
	l && Ze(e.attrs, "set", "");
}
function ai(e, n, r, i) {
	let [a, o] = e.propsOptions, s = !1, c;
	if (n) for (let t in n) {
		if (ee(t)) continue;
		let l = n[t], d;
		a && u(a, d = T(t)) ? !o || !o.includes(d) ? r[d] = l : (c ||= {})[d] = l : Kr(e.emitsOptions, t) || (!(t in i) || l !== i[t]) && (i[t] = l, s = !0);
	}
	if (o) {
		let n = /* @__PURE__ */ I(r), i = c || t;
		for (let t = 0; t < o.length; t++) {
			let s = o[t];
			r[s] = oi(a, n, s, i[s], e, !u(i, s));
		}
	}
	return s;
}
function oi(e, t, n, r, i, a) {
	let o = e[n];
	if (o != null) {
		let e = u(o, "default");
		if (e && r === void 0) {
			let e = o.default;
			if (o.type !== Function && !o.skipFactory && h(e)) {
				let { propsDefaults: a } = i;
				if (n in a) r = a[n];
				else {
					let o = ra(i);
					r = a[n] = e.call(null, t), o();
				}
			} else r = e;
			i.ce && i.ce._setProp(n, r);
		}
		o[0] && (a && !e ? r = !1 : o[1] && (r === "" || r === E(n)) && (r = !0));
	}
	return r;
}
var si = /* @__PURE__ */ new WeakMap();
function ci(e, r, i = !1) {
	let a = i ? si : r.propsCache, o = a.get(e);
	if (o) return o;
	let c = e.props, l = {}, f = [], p = !1;
	if (!h(e)) {
		let t = (e) => {
			p = !0;
			let [t, n] = ci(e, r, !0);
			s(l, t), n && f.push(...n);
		};
		!i && r.mixins.length && r.mixins.forEach(t), e.extends && t(e.extends), e.mixins && e.mixins.forEach(t);
	}
	if (!c && !p) return v(e) && a.set(e, n), n;
	if (d(c)) for (let e = 0; e < c.length; e++) {
		let n = T(c[e]);
		li(n) && (l[n] = t);
	}
	else if (c) for (let e in c) {
		let t = T(e);
		if (li(t)) {
			let n = c[e], r = l[t] = d(n) || h(n) ? { type: n } : s({}, n), i = r.type, a = !1, o = !0;
			if (d(i)) for (let e = 0; e < i.length; ++e) {
				let t = i[e], n = h(t) && t.name;
				if (n === "Boolean") {
					a = !0;
					break;
				}
				n === "String" && (o = !1);
			}
			else a = h(i) && i.name === "Boolean";
			r[0] = a, r[1] = o, (a || u(r, "default")) && f.push(t);
		}
	}
	let m = [l, f];
	return v(e) && a.set(e, m), m;
}
function li(e) {
	return e[0] !== "$" && !ee(e);
}
var ui = (e) => e === "_" || e === "_ctx" || e === "$stable", di = (e) => d(e) ? e.map(X) : [X(e)], fi = (e, t, n) => {
	if (t._n) return t;
	let r = Sn((...e) => di(t(...e)), n);
	return r._c = !1, r;
}, pi = (e, t, n) => {
	let r = e._ctx;
	for (let n in e) {
		if (ui(n)) continue;
		let i = e[n];
		if (h(i)) t[n] = fi(n, i, r);
		else if (i != null) {
			let e = di(i);
			t[n] = () => e;
		}
	}
}, mi = (e, t) => {
	let n = di(t);
	e.slots.default = () => n;
}, hi = (e, t, n) => {
	for (let r in t) (n || !ui(r)) && (e[r] = t[r]);
}, gi = (e, t, n) => {
	let r = e.slots = ti();
	if (e.vnode.shapeFlag & 32) {
		let e = t._;
		e ? (hi(r, t, n), n && O(r, "_", e, !0)) : pi(t, r);
	} else t && mi(e, t);
}, _i = (e, n, r) => {
	let { vnode: i, slots: a } = e, o = !0, s = t;
	if (i.shapeFlag & 32) {
		let e = n._;
		e ? r && e === 1 ? o = !1 : hi(a, n, r) : (o = !n.$stable, pi(n, a)), s = n;
	} else n && (mi(e, n), s = { default: 1 });
	if (o) for (let e in a) !ui(e) && s[e] == null && delete a[e];
}, U = ki;
function vi(e) {
	return yi(e);
}
function yi(e, i) {
	let a = le();
	a.__VUE__ = !0;
	let { insert: o, remove: s, patchProp: c, createElement: l, createText: u, createComment: d, setText: f, setElementText: p, parentNode: m, nextSibling: h, setScopeId: g = r, insertStaticContent: _ } = e, v = (e, t, n, r = null, i = null, a = null, o = void 0, s = null, c = !!t.dynamicChildren) => {
		if (e === t) return;
		e && !Bi(e, t) && (r = ye(e), k(e, i, a, !0), e = null), t.patchFlag === -2 && (c = !1, t.dynamicChildren = null);
		let { type: l, ref: u, shapeFlag: d } = t;
		switch (l) {
			case Ai:
				y(e, t, n, r);
				break;
			case ji:
				b(e, t, n, r);
				break;
			case Mi:
				e ?? x(t, n, r, o);
				break;
			case W:
				ae(e, t, n, r, i, a, o, s, c);
				break;
			default: d & 1 ? w(e, t, n, r, i, a, o, s, c) : d & 6 ? D(e, t, n, r, i, a, o, s, c) : (d & 64 || d & 128) && l.process(e, t, n, r, i, a, o, s, c, xe);
		}
		u != null && i ? Qn(u, e && e.ref, a, t || e, !t) : u == null && e && e.ref != null && Qn(e.ref, null, a, e, !0);
	}, y = (e, t, n, r) => {
		if (e == null) o(t.el = u(t.children), n, r);
		else {
			let n = t.el = e.el;
			t.children !== e.children && f(n, t.children);
		}
	}, b = (e, t, n, r) => {
		e == null ? o(t.el = d(t.children || ""), n, r) : t.el = e.el;
	}, x = (e, t, n, r) => {
		[e.el, e.anchor] = _(e.children, t, n, r, e.el, e.anchor);
	}, S = ({ el: e, anchor: t }, n, r) => {
		let i;
		for (; e && e !== t;) i = h(e), o(e, n, r), e = i;
		o(t, n, r);
	}, C = ({ el: e, anchor: t }) => {
		let n;
		for (; e && e !== t;) n = h(e), s(e), e = n;
		s(t);
	}, w = (e, t, n, r, i, a, o, s, c) => {
		if (t.type === "svg" ? o = "svg" : t.type === "math" && (o = "mathml"), e == null) te(t, n, r, i, a, o, s, c);
		else {
			let n = e.el && e.el._isVueCE ? e.el : null;
			try {
				n && n._beginPatch(), re(e, t, i, a, o, s, c);
			} finally {
				n && n._endPatch();
			}
		}
	}, te = (e, t, n, r, i, a, s, u) => {
		let d, f, { props: m, shapeFlag: h, transition: g, dirs: _ } = e;
		if (d = e.el = l(e.type, a, m && m.is, m), h & 8 ? p(d, e.children) : h & 16 && T(e.children, d, null, r, i, bi(e, a), s, u), _ && Cn(e, null, r, "created"), ne(d, e, e.scopeId, s, r), m) {
			for (let e in m) e !== "value" && !ee(e) && c(d, e, null, m[e], a, r);
			"value" in m && c(d, "value", null, m.value, a), (f = m.onVnodeBeforeMount) && Z(f, r, e);
		}
		_ && Cn(e, null, r, "beforeMount");
		let v = Si(i, g);
		v && g.beforeEnter(d), o(d, t, n), ((f = m && m.onVnodeMounted) || v || _) && U(() => {
			try {
				f && Z(f, r, e), v && g.enter(d), _ && Cn(e, null, r, "mounted");
			} finally {}
		}, i);
	}, ne = (e, t, n, r, i) => {
		if (n && g(e, n), r) for (let t = 0; t < r.length; t++) g(e, r[t]);
		if (i) {
			let n = i.subTree;
			if (t === n || Oi(n.type) && (n.ssContent === t || n.ssFallback === t)) {
				let t = i.vnode;
				ne(e, t, t.scopeId, t.slotScopeIds, i.parent);
			}
		}
	}, T = (e, t, n, r, i, a, o, s, c = 0) => {
		for (let l = c; l < e.length; l++) {
			let c = e[l] = s ? Ji(e[l]) : X(e[l]);
			v(null, c, t, n, r, i, a, o, s);
		}
	}, re = (e, n, r, i, a, o, s) => {
		let l = n.el = e.el, { patchFlag: u, dynamicChildren: d, dirs: f } = n;
		u |= e.patchFlag & 16;
		let m = e.props || t, h = n.props || t, g;
		if (r && xi(r, !1), (g = h.onVnodeBeforeUpdate) && Z(g, r, n, e), f && Cn(n, e, r, "beforeUpdate"), r && xi(r, !0), d && (!e.dynamicChildren || e.dynamicChildren.length !== d.length) && (u = 0, s = !1, d = null), (m.innerHTML && h.innerHTML == null || m.textContent && h.textContent == null) && p(l, ""), d ? E(e.dynamicChildren, d, l, r, i, bi(n, a), o) : s || de(e, n, l, null, r, i, bi(n, a), o, !1), u > 0) {
			if (u & 16) ie(l, m, h, r, a);
			else if (u & 2 && m.class !== h.class && c(l, "class", null, h.class, a), u & 4 && c(l, "style", m.style, h.style, a), u & 8) {
				let e = n.dynamicProps;
				for (let t = 0; t < e.length; t++) {
					let n = e[t], i = m[n], o = h[n];
					(o !== i || n === "value") && c(l, n, i, o, a, r);
				}
			}
			u & 1 && e.children !== n.children && p(l, n.children);
		} else !s && d == null && ie(l, m, h, r, a);
		((g = h.onVnodeUpdated) || f) && U(() => {
			g && Z(g, r, n, e), f && Cn(n, e, r, "updated");
		}, i);
	}, E = (e, t, n, r, i, a, o) => {
		for (let s = 0; s < t.length; s++) {
			let c = e[s], l = t[s], u = c.el && (c.type === W || !Bi(c, l) || c.shapeFlag & 198) ? m(c.el) : n;
			v(c, l, u, null, r, i, a, o, !0);
		}
	}, ie = (e, n, r, i, a) => {
		if (n !== r) {
			if (n !== t) for (let t in n) !ee(t) && !(t in r) && c(e, t, n[t], null, a, i);
			for (let t in r) {
				if (ee(t)) continue;
				let o = r[t], s = n[t];
				o !== s && t !== "value" && c(e, t, s, o, a, i);
			}
			"value" in r && c(e, "value", n.value, r.value, a);
		}
	}, ae = (e, t, n, r, i, a, s, c, l) => {
		let d = t.el = e ? e.el : u(""), f = t.anchor = e ? e.anchor : u(""), { patchFlag: p, dynamicChildren: m, slotScopeIds: h } = t;
		h && (c = c ? c.concat(h) : h), e == null ? (o(d, n, r), o(f, n, r), T(t.children || [], n, f, i, a, s, c, l)) : p > 0 && p & 64 && m && e.dynamicChildren && e.dynamicChildren.length === m.length ? (E(e.dynamicChildren, m, n, i, a, s, c), (t.key != null || i && t === i.subTree) && Ci(e, t, !0)) : de(e, t, n, f, i, a, s, c, l);
	}, D = (e, t, n, r, i, a, o, s, c) => {
		t.slotScopeIds = s, e == null ? t.shapeFlag & 512 ? i.ctx.activate(t, n, r, o, c) : O(t, n, r, i, a, o, c) : se(e, t, c);
	}, O = (e, t, n, r, i, a, o) => {
		let s = e.component = $i(e, r, i);
		if (tr(e) && (s.ctx.renderer = xe), sa(s, !1, o), s.asyncDep) {
			if (i && i.registerDep(s, ce, o), !e.el) {
				let r = s.subTree = Ui(ji);
				b(null, r, t, n), e.placeholder = r.el;
			}
		} else ce(s, e, t, n, i, a, o);
	}, se = (e, t, n) => {
		let r = t.component = e.component;
		if (Xr(e, t, n)) if (r.asyncDep && !r.asyncResolved) {
			ue(r, t, n);
			return;
		} else r.next = t, r.update();
		else t.el = e.el, r.vnode = t;
	}, ce = (e, t, n, r, i, a, o) => {
		let s = () => {
			if (e.isMounted) {
				let { next: t, bu: n, u: r, parent: s, vnode: c } = e;
				{
					let n = Ti(e);
					if (n) {
						t && (t.el = c.el, ue(e, t, o)), n.asyncDep.then(() => {
							U(() => {
								e.isUnmounted || l();
							}, i);
						});
						return;
					}
				}
				let u = t, d;
				xi(e, !1), t ? (t.el = c.el, ue(e, t, o)) : t = c, n && oe(n), (d = t.props && t.props.onVnodeBeforeUpdate) && Z(d, s, t, c), xi(e, !0);
				let f = qr(e), p = e.subTree;
				e.subTree = f, v(p, f, m(p.el), ye(p), e, i, a), t.el = f.el, u === null && $r(e, f.el), r && U(r, i), (d = t.props && t.props.onVnodeUpdated) && U(() => Z(d, s, t, c), i);
			} else {
				let o, { el: s, props: c } = t, { bm: l, m: u, parent: d, root: f, type: p } = e, m = er(t);
				if (xi(e, !1), l && oe(l), !m && (o = c && c.onVnodeBeforeMount) && Z(o, d, t), xi(e, !0), s && j) {
					let t = () => {
						e.subTree = qr(e), j(s, e.subTree, e, i, null);
					};
					m && p.__asyncHydrate ? p.__asyncHydrate(s, e, t) : t();
				} else {
					f.ce && f.ce._hasShadowRoot() && f.ce._injectChildStyle(p, e.parent ? e.parent.type : void 0);
					let o = e.subTree = qr(e);
					v(null, o, n, r, e, i, a), t.el = o.el;
				}
				if (u && U(u, i), !m && (o = c && c.onVnodeMounted)) {
					let e = t;
					U(() => Z(o, d, e), i);
				}
				(t.shapeFlag & 256 || d && er(d.vnode) && d.vnode.shapeFlag & 256) && e.a && U(e.a, i), e.isMounted = !0, t = n = r = null;
			}
		};
		e.scope.on();
		let c = e.effect = new Ee(s);
		e.scope.off();
		let l = e.update = c.run.bind(c), u = e.job = c.runIfDirty.bind(c);
		u.i = e, u.id = e.uid, c.scheduler = () => fn(u), xi(e, !0), l();
	}, ue = (e, t, n) => {
		t.component = e;
		let r = e.vnode.props;
		e.vnode = t, e.next = null, ii(e, t.props, r, n), _i(e, t.children, n), Be(), hn(e), Ve();
	}, de = (e, t, n, r, i, a, o, s, c = !1) => {
		let l = e && e.children, u = e ? e.shapeFlag : 0, d = t.children, { patchFlag: f, shapeFlag: m } = t;
		if (f > 0) {
			if (f & 128) {
				pe(l, d, n, r, i, a, o, s, c);
				return;
			}
			if (f & 256) {
				fe(l, d, n, r, i, a, o, s, c);
				return;
			}
		}
		m & 8 ? (u & 16 && ve(l, i, a), d !== l && p(n, d)) : u & 16 ? m & 16 ? pe(l, d, n, r, i, a, o, s, c) : ve(l, i, a, !0) : (u & 8 && p(n, ""), m & 16 && T(d, n, r, i, a, o, s, c));
	}, fe = (e, t, r, i, a, o, s, c, l) => {
		e ||= n, t ||= n;
		let u = e.length, d = t.length, f = Math.min(u, d), p;
		for (p = 0; p < f; p++) {
			let n = t[p] = l ? Ji(t[p]) : X(t[p]);
			v(e[p], n, r, null, a, o, s, c, l);
		}
		u > d ? ve(e, a, o, !0, !1, f) : T(t, r, i, a, o, s, c, l, f);
	}, pe = (e, t, r, i, a, o, s, c, l) => {
		let u = 0, d = t.length, f = e.length - 1, p = d - 1;
		for (; u <= f && u <= p;) {
			let n = e[u], i = t[u] = l ? Ji(t[u]) : X(t[u]);
			if (Bi(n, i)) v(n, i, r, null, a, o, s, c, l);
			else break;
			u++;
		}
		for (; u <= f && u <= p;) {
			let n = e[f], i = t[p] = l ? Ji(t[p]) : X(t[p]);
			if (Bi(n, i)) v(n, i, r, null, a, o, s, c, l);
			else break;
			f--, p--;
		}
		if (u > f) {
			if (u <= p) {
				let e = p + 1, n = e < d ? t[e].el : i;
				for (; u <= p;) v(null, t[u] = l ? Ji(t[u]) : X(t[u]), r, n, a, o, s, c, l), u++;
			}
		} else if (u > p) for (; u <= f;) k(e[u], a, o, !0), u++;
		else {
			let m = u, h = u, g = /* @__PURE__ */ new Map();
			for (u = h; u <= p; u++) {
				let e = t[u] = l ? Ji(t[u]) : X(t[u]);
				e.key != null && g.set(e.key, u);
			}
			let _, y = 0, b = p - h + 1, x = !1, S = 0, C = Array(b);
			for (u = 0; u < b; u++) C[u] = 0;
			for (u = m; u <= f; u++) {
				let n = e[u];
				if (y >= b) {
					k(n, a, o, !0);
					continue;
				}
				let i;
				if (n.key != null) i = g.get(n.key);
				else for (_ = h; _ <= p; _++) if (C[_ - h] === 0 && Bi(n, t[_])) {
					i = _;
					break;
				}
				i === void 0 ? k(n, a, o, !0) : (C[i - h] = u + 1, i >= S ? S = i : x = !0, v(n, t[i], r, null, a, o, s, c, l), y++);
			}
			let w = x ? wi(C) : n;
			for (_ = w.length - 1, u = b - 1; u >= 0; u--) {
				let e = h + u, n = t[e], f = t[e + 1], p = e + 1 < d ? f.el || Di(f) : i;
				C[u] === 0 ? v(null, n, r, p, a, o, s, c, l) : x && (_ < 0 || u !== w[_] ? me(n, r, p, 2) : _--);
			}
		}
	}, me = (e, t, n, r, i = null) => {
		let { el: a, type: c, transition: l, children: u, shapeFlag: d } = e;
		if (d & 6) {
			me(e.component.subTree, t, n, r);
			return;
		}
		if (d & 128) {
			e.suspense.move(t, n, r);
			return;
		}
		if (d & 64) {
			c.move(e, t, n, xe);
			return;
		}
		if (c === W) {
			o(a, t, n);
			for (let e = 0; e < u.length; e++) me(u[e], t, n, r);
			o(e.anchor, t, n);
			return;
		}
		if (c === Mi) {
			S(e, t, n);
			return;
		}
		if (r !== 2 && d & 1 && l) if (r === 0) l.persisted && !a[Kn] ? o(a, t, n) : (l.beforeEnter(a), o(a, t, n), U(() => l.enter(a), i));
		else {
			let { leave: r, delayLeave: i, afterLeave: c } = l, u = () => {
				e.ctx.isUnmounted ? s(a) : o(a, t, n);
			}, d = () => {
				let e = a._isLeaving || !!a[Kn];
				a._isLeaving && a[Kn](!0), l.persisted && !e ? u() : r(a, () => {
					u(), c && c();
				});
			};
			i ? i(a, u, d) : d();
		}
		else o(a, t, n);
	}, k = (e, t, n, r = !1, i = !1) => {
		let { type: a, props: o, ref: s, children: c, dynamicChildren: l, shapeFlag: u, patchFlag: d, dirs: f, cacheIndex: p, memo: m } = e;
		if (d === -2 && (i = !1), s != null && (Be(), Qn(s, null, n, e, !0), Ve()), p != null && (t.renderCache[p] = void 0), u & 256) {
			t.ctx.deactivate(e);
			return;
		}
		let h = u & 1 && f, g = !er(e), _;
		if (g && (_ = o && o.onVnodeBeforeUnmount) && Z(_, t, e), u & 6) _e(e.component, n, r);
		else {
			if (u & 128) {
				e.suspense.unmount(n, r);
				return;
			}
			h && Cn(e, null, t, "beforeUnmount"), u & 64 ? e.type.remove(e, t, n, xe, r) : l && !l.hasOnce && (a !== W || d > 0 && d & 64) ? ve(l, t, n, !1, !0) : (a === W && d & 384 || !i && u & 16) && ve(c, t, n), r && he(e);
		}
		let v = m != null && p == null;
		(g && (_ = o && o.onVnodeUnmounted) || h || v) && U(() => {
			_ && Z(_, t, e), h && Cn(e, null, t, "unmounted"), v && (e.el = null);
		}, n);
	}, he = (e) => {
		let { type: t, el: n, anchor: r, transition: i } = e;
		if (t === W) {
			ge(n, r);
			return;
		}
		if (t === Mi) {
			C(e);
			return;
		}
		let a = () => {
			s(n), i && !i.persisted && i.afterLeave && i.afterLeave();
		};
		if (e.shapeFlag & 1 && i && !i.persisted) {
			let { leave: t, delayLeave: r } = i, o = () => t(n, a);
			r ? r(e.el, a, o) : o();
		} else a();
	}, ge = (e, t) => {
		let n;
		for (; e !== t;) n = h(e), s(e), e = n;
		s(t);
	}, _e = (e, t, n) => {
		let { bum: r, scope: i, job: a, subTree: o, um: s, m: c, a: l } = e;
		Ei(c), Ei(l), r && oe(r), i.stop(), a && (a.flags |= 8, k(o, e, t, n)), s && U(s, t), U(() => {
			e.isUnmounted = !0;
		}, t);
	}, ve = (e, t, n, r = !1, i = !1, a = 0) => {
		for (let o = a; o < e.length; o++) k(e[o], t, n, r, i);
	}, ye = (e) => {
		if (e.shapeFlag & 6) return ye(e.component.subTree);
		if (e.shapeFlag & 128) return e.suspense.next();
		let t = h(e.anchor || e.el), n = t && t[Nn];
		return n ? h(n) : t;
	}, be = !1, A = (e, t, n) => {
		let r;
		e == null ? t._vnode && (k(t._vnode, null, null, !0), r = t._vnode.component) : v(t._vnode || null, e, t, null, null, null, n), t._vnode = e, be ||= (be = !0, hn(r), gn(), !1);
	}, xe = {
		p: v,
		um: k,
		m: me,
		r: he,
		mt: O,
		mc: T,
		pc: de,
		pbc: E,
		n: ye,
		o: e
	}, Se, j;
	return i && ([Se, j] = i(xe)), {
		render: A,
		hydrate: Se,
		createApp: Br(A, Se)
	};
}
function bi({ type: e, props: t }, n) {
	return n === "svg" && e === "foreignObject" || n === "mathml" && e === "annotation-xml" && t && t.encoding && t.encoding.includes("html") ? void 0 : n;
}
function xi({ effect: e, job: t }, n) {
	n ? (e.flags |= 32, t.flags |= 4) : (e.flags &= -33, t.flags &= -5);
}
function Si(e, t) {
	return (!e || e && !e.pendingBranch) && t && !t.persisted;
}
function Ci(e, t, n = !1) {
	let r = e.children, i = t.children;
	if (d(r) && d(i)) for (let e = 0; e < r.length; e++) {
		let t = r[e], a = i[e];
		a.shapeFlag & 1 && !a.dynamicChildren && ((a.patchFlag <= 0 || a.patchFlag === 32) && (a = i[e] = Ji(i[e]), a.el = t.el), !n && a.patchFlag !== -2 && Ci(t, a)), a.type === Ai && (a.patchFlag === -1 && (a = i[e] = Ji(a)), a.el = t.el), a.type === ji && !a.el && (a.el = t.el);
	}
}
function wi(e) {
	let t = e.slice(), n = [0], r, i, a, o, s, c = e.length;
	for (r = 0; r < c; r++) {
		let c = e[r];
		if (c !== 0) {
			if (i = n[n.length - 1], e[i] < c) {
				t[r] = i, n.push(r);
				continue;
			}
			for (a = 0, o = n.length - 1; a < o;) s = a + o >> 1, e[n[s]] < c ? a = s + 1 : o = s;
			c < e[n[a]] && (a > 0 && (t[r] = n[a - 1]), n[a] = r);
		}
	}
	for (a = n.length, o = n[a - 1]; a-- > 0;) n[a] = o, o = t[o];
	return n;
}
function Ti(e) {
	let t = e.subTree.component;
	if (t) return t.asyncDep && !t.asyncResolved ? t : Ti(t);
}
function Ei(e) {
	if (e) for (let t = 0; t < e.length; t++) e[t].flags |= 8;
}
function Di(e) {
	if (e.placeholder) return e.placeholder;
	let t = e.component;
	return t ? Di(t.subTree) : null;
}
var Oi = (e) => e.__isSuspense;
function ki(e, t) {
	t && t.pendingBranch ? d(e) ? t.effects.push(...e) : t.effects.push(e) : mn(e);
}
var W = /* @__PURE__ */ Symbol.for("v-fgt"), Ai = /* @__PURE__ */ Symbol.for("v-txt"), ji = /* @__PURE__ */ Symbol.for("v-cmt"), Mi = /* @__PURE__ */ Symbol.for("v-stc"), Ni = [], G = null;
function K(e = !1) {
	Ni.push(G = e ? null : []);
}
function Pi() {
	Ni.pop(), G = Ni[Ni.length - 1] || null;
}
var Fi = 1;
function Ii(e, t = !1) {
	Fi += e, e < 0 && G && t && (G.hasOnce = !0);
}
function Li(e) {
	return e.dynamicChildren = Fi > 0 ? G || n : null, Pi(), Fi > 0 && G && G.push(e), e;
}
function q(e, t, n, r, i, a) {
	return Li(J(e, t, n, r, i, a, !0));
}
function Ri(e, t, n, r, i) {
	return Li(Ui(e, t, n, r, i, !0));
}
function zi(e) {
	return e ? e.__v_isVNode === !0 : !1;
}
function Bi(e, t) {
	return e.type === t.type && e.key === t.key;
}
var Vi = ({ key: e }) => e ?? null, Hi = ({ ref: e, ref_key: t, ref_for: n }) => (typeof e == "number" && (e = "" + e), e == null ? null : g(e) || /* @__PURE__ */ R(e) || h(e) ? {
	i: yn,
	r: e,
	k: t,
	f: !!n
} : e);
function J(e, t = null, n = null, r = 0, i = null, a = e === W ? 0 : 1, o = !1, s = !1) {
	let c = {
		__v_isVNode: !0,
		__v_skip: !0,
		type: e,
		props: t,
		key: t && Vi(t),
		ref: t && Hi(t),
		scopeId: bn,
		slotScopeIds: null,
		children: n,
		component: null,
		suspense: null,
		ssContent: null,
		ssFallback: null,
		dirs: null,
		transition: null,
		el: null,
		anchor: null,
		target: null,
		targetStart: null,
		targetAnchor: null,
		staticCount: 0,
		shapeFlag: a,
		patchFlag: r,
		dynamicProps: i,
		dynamicChildren: null,
		appContext: null,
		ctx: yn
	};
	return s ? (Yi(c, n), a & 128 && e.normalize(c)) : n && (c.shapeFlag |= g(n) ? 8 : 16), Fi > 0 && !o && G && (c.patchFlag > 0 || a & 6) && c.patchFlag !== 32 && G.push(c), c;
}
var Ui = Wi;
function Wi(e, t = null, n = null, r = 0, i = null, a = !1) {
	if ((!e || e === vr) && (e = ji), zi(e)) {
		let r = Ki(e, t, !0);
		return n && Yi(r, n), Fi > 0 && !a && G && (r.shapeFlag & 6 ? G[G.indexOf(e)] = r : G.push(r)), r.patchFlag = -2, r;
	}
	if (ga(e) && (e = e.__vccOpts), t) {
		t = Gi(t);
		let { class: e, style: n } = t;
		e && !g(e) && (t.class = k(e)), v(n) && (/* @__PURE__ */ Lt(n) && !d(n) && (n = s({}, n)), t.style = ue(n));
	}
	let o = g(e) ? 1 : Oi(e) ? 128 : Pn(e) ? 64 : v(e) ? 4 : h(e) ? 2 : 0;
	return J(e, t, n, r, i, o, a, !0);
}
function Gi(e) {
	return e ? /* @__PURE__ */ Lt(e) || ni(e) ? s({}, e) : e : null;
}
function Ki(e, t, n = !1, r = !1) {
	let { props: i, ref: a, patchFlag: o, children: s, transition: c } = e, l = t ? Xi(i || {}, t) : i, u = {
		__v_isVNode: !0,
		__v_skip: !0,
		type: e.type,
		props: l,
		key: l && Vi(l),
		ref: t && t.ref ? n && a ? d(a) ? a.concat(Hi(t)) : [a, Hi(t)] : Hi(t) : a,
		scopeId: e.scopeId,
		slotScopeIds: e.slotScopeIds,
		children: s,
		target: e.target,
		targetStart: e.targetStart,
		targetAnchor: e.targetAnchor,
		staticCount: e.staticCount,
		shapeFlag: e.shapeFlag,
		patchFlag: t && e.type !== W ? o === -1 ? 16 : o | 16 : o,
		dynamicProps: e.dynamicProps,
		dynamicChildren: e.dynamicChildren,
		appContext: e.appContext,
		dirs: e.dirs,
		transition: c,
		component: e.component,
		suspense: e.suspense,
		ssContent: e.ssContent && Ki(e.ssContent),
		ssFallback: e.ssFallback && Ki(e.ssFallback),
		placeholder: e.placeholder,
		el: e.el,
		anchor: e.anchor,
		ctx: e.ctx,
		ce: e.ce
	};
	return c && r && qn(u, c.clone(u)), u;
}
function qi(e = " ", t = 0) {
	return Ui(Ai, null, e, t);
}
function Y(e = "", t = !1) {
	return t ? (K(), Ri(ji, null, e)) : Ui(ji, null, e);
}
function X(e) {
	return e == null || typeof e == "boolean" ? Ui(ji) : d(e) ? Ui(W, null, e.slice()) : zi(e) ? Ji(e) : Ui(Ai, null, String(e));
}
function Ji(e) {
	return e.el === null && e.patchFlag !== -1 || e.memo ? e : Ki(e);
}
function Yi(e, t) {
	let n = 0, { shapeFlag: r } = e;
	if (t == null) t = null;
	else if (d(t)) n = 16;
	else if (typeof t == "object") if (r & 65) {
		let n = t.default;
		n && (n._c && (n._d = !1), Yi(e, n()), n._c && (n._d = !0));
		return;
	} else {
		n = 32;
		let r = t._;
		!r && !ni(t) ? t._ctx = yn : r === 3 && yn && (yn.slots._ === 1 ? t._ = 1 : (t._ = 2, e.patchFlag |= 1024));
	}
	else if (h(t)) {
		if (r & 65) {
			Yi(e, { default: t });
			return;
		}
		t = {
			default: t,
			_ctx: yn
		}, n = 32;
	} else t = String(t), r & 64 ? (n = 16, t = [qi(t)]) : n = 8;
	e.children = t, e.shapeFlag |= n;
}
function Xi(...e) {
	let t = {};
	for (let n = 0; n < e.length; n++) {
		let r = e[n];
		for (let e in r) if (e === "class") t.class !== r.class && (t.class = k([t.class, r.class]));
		else if (e === "style") t.style = ue([t.style, r.style]);
		else if (a(e)) {
			let n = t[e], i = r[e];
			i && n !== i && !(d(n) && n.includes(i)) ? t[e] = n ? [].concat(n, i) : i : i == null && n == null && !o(e) && (t[e] = i);
		} else e !== "" && (t[e] = r[e]);
	}
	return t;
}
function Z(e, t, n, r = null) {
	z(e, t, 7, [n, r]);
}
var Zi = Rr(), Qi = 0;
function $i(e, n, r) {
	let i = e.type, a = (n ? n.appContext : e.appContext) || Zi, o = {
		uid: Qi++,
		vnode: e,
		type: i,
		parent: n,
		appContext: a,
		root: null,
		next: null,
		subTree: null,
		effect: null,
		update: null,
		job: null,
		scope: new Ce(!0),
		render: null,
		proxy: null,
		exposed: null,
		exposeProxy: null,
		withProxy: null,
		provides: n ? n.provides : Object.create(a.provides),
		ids: n ? n.ids : [
			"",
			0,
			0
		],
		accessCache: null,
		renderCache: [],
		components: null,
		directives: null,
		propsOptions: ci(i, a),
		emitsOptions: Gr(i, a),
		emit: null,
		emitted: null,
		propsDefaults: t,
		inheritAttrs: i.inheritAttrs,
		ctx: t,
		data: t,
		props: t,
		attrs: t,
		slots: t,
		refs: t,
		setupState: t,
		setupContext: null,
		suspense: r,
		suspenseId: r ? r.pendingId : 0,
		asyncDep: null,
		asyncResolved: !1,
		isMounted: !1,
		isUnmounted: !1,
		isDeactivated: !1,
		bc: null,
		c: null,
		bm: null,
		m: null,
		bu: null,
		u: null,
		um: null,
		bum: null,
		da: null,
		a: null,
		rtg: null,
		rtc: null,
		ec: null,
		sp: null
	};
	return o.ctx = { _: o }, o.root = n ? n.root : o, o.emit = Ur.bind(null, o), e.ce && e.ce(o), o;
}
var Q = null, ea = () => Q || yn, ta, na;
{
	let e = le(), t = (t, n) => {
		let r;
		return (r = e[t]) || (r = e[t] = []), r.push(n), (e) => {
			r.length > 1 ? r.forEach((t) => t(e)) : r[0](e);
		};
	};
	ta = t("__VUE_INSTANCE_SETTERS__", (e) => Q = e), na = t("__VUE_SSR_SETTERS__", (e) => oa = e);
}
var ra = (e) => {
	let t = Q;
	return ta(e), e.scope.on(), () => {
		e.scope.off(), ta(t);
	};
}, ia = () => {
	Q && Q.scope.off(), ta(null);
};
function aa(e) {
	return e.vnode.shapeFlag & 4;
}
var oa = !1;
function sa(e, t = !1, n = !1) {
	t && na(t);
	let { props: r, children: i } = e.vnode, a = aa(e);
	ri(e, r, a, t), gi(e, i, n || t);
	let o = a ? ca(e, t) : void 0;
	return t && na(!1), o;
}
function ca(e, t) {
	let n = e.type;
	e.accessCache = /* @__PURE__ */ Object.create(null), e.proxy = new Proxy(e.ctx, Sr);
	let { setup: r } = n;
	if (r) {
		Be();
		let n = e.setupContext = r.length > 1 ? ma(e) : null, i = ra(e), a = en(r, e, 0, [e.props, n]), o = y(a);
		if (Ve(), i(), (o || e.sp) && !er(e) && Yn(e), o) {
			if (a.then(ia, ia), t) return a.then((n) => {
				la(e, n, t);
			}).catch((t) => {
				tn(t, e, 0);
			});
			e.asyncDep = a;
		} else la(e, a, t);
	} else fa(e, t);
}
function la(e, t, n) {
	h(t) ? e.type.__ssrInlineRender ? e.ssrRender = t : e.render = t : v(t) && (e.setupState = Gt(t)), fa(e, n);
}
var ua, da;
function fa(e, t, n) {
	let i = e.type;
	if (!e.render) {
		if (!t && ua && !i.render) {
			let t = i.template || kr(e).template;
			if (t) {
				let { isCustomElement: n, compilerOptions: r } = e.appContext.config, { delimiters: a, compilerOptions: o } = i;
				i.render = ua(t, s(s({
					isCustomElement: n,
					delimiters: a
				}, r), o));
			}
		}
		e.render = i.render || r, da && da(e);
	}
	{
		let t = ra(e);
		Be();
		try {
			Tr(e);
		} finally {
			Ve(), t();
		}
	}
}
var pa = { get(e, t) {
	return P(e, "get", ""), e[t];
} };
function ma(e) {
	return {
		attrs: new Proxy(e.attrs, pa),
		slots: e.slots,
		emit: e.emit,
		expose: (t) => {
			e.exposed = t || {};
		}
	};
}
function ha(e) {
	return e.exposed ? e.exposeProxy ||= new Proxy(Gt(Rt(e.exposed)), {
		get(t, n) {
			if (n in t) return t[n];
			if (n in br) return br[n](e);
		},
		has(e, t) {
			return t in e || t in br;
		}
	}) : e.proxy;
}
function ga(e) {
	return h(e) && "__vccOpts" in e;
}
var $ = (e, t) => /* @__PURE__ */ qt(e, t, oa), _a = "3.5.40", va = void 0, ya = typeof window < "u" && window.trustedTypes;
if (ya) try {
	va = /* @__PURE__ */ ya.createPolicy("vue", { createHTML: (e) => e });
} catch {}
var ba = va ? (e) => va.createHTML(e) : (e) => e, xa = "http://www.w3.org/2000/svg", Sa = "http://www.w3.org/1998/Math/MathML", Ca = typeof document < "u" ? document : null, wa = Ca && /* @__PURE__ */ Ca.createElement("template"), Ta = {
	insert: (e, t, n) => {
		t.insertBefore(e, n || null);
	},
	remove: (e) => {
		let t = e.parentNode;
		t && t.removeChild(e);
	},
	createElement: (e, t, n, r) => {
		let i = t === "svg" ? Ca.createElementNS(xa, e) : t === "mathml" ? Ca.createElementNS(Sa, e) : n ? Ca.createElement(e, { is: n }) : Ca.createElement(e);
		return e === "select" && r && r.multiple != null && i.setAttribute("multiple", r.multiple), i;
	},
	createText: (e) => Ca.createTextNode(e),
	createComment: (e) => Ca.createComment(e),
	setText: (e, t) => {
		e.nodeValue = t;
	},
	setElementText: (e, t) => {
		e.textContent = t;
	},
	parentNode: (e) => e.parentNode,
	nextSibling: (e) => e.nextSibling,
	querySelector: (e) => Ca.querySelector(e),
	setScopeId(e, t) {
		e.setAttribute(t, "");
	},
	insertStaticContent(e, t, n, r, i, a) {
		let o = n ? n.previousSibling : t.lastChild;
		if (i && (i === a || i.nextSibling)) for (; t.insertBefore(i.cloneNode(!0), n), !(i === a || !(i = i.nextSibling)););
		else {
			wa.innerHTML = ba(r === "svg" ? `<svg>${e}</svg>` : r === "mathml" ? `<math>${e}</math>` : e);
			let i = wa.content;
			if (r === "svg" || r === "mathml") {
				let e = i.firstChild;
				for (; e.firstChild;) i.appendChild(e.firstChild);
				i.removeChild(e);
			}
			t.insertBefore(i, n);
		}
		return [o ? o.nextSibling : t.firstChild, n ? n.previousSibling : t.lastChild];
	}
}, Ea = /* @__PURE__ */ Symbol("_vtc");
function Da(e, t, n) {
	let r = e[Ea];
	r && (t = (t ? [t, ...r] : [...r]).join(" ")), t == null ? e.removeAttribute("class") : n ? e.setAttribute("class", t) : e.className = t;
}
var Oa = /* @__PURE__ */ Symbol("_vod"), ka = /* @__PURE__ */ Symbol("_vsh"), Aa = /* @__PURE__ */ Symbol(""), ja = /(?:^|;)\s*display\s*:/;
function Ma(e, t, n) {
	let r = e.style, i = g(n), a = !1;
	if (n && !i) {
		if (t) if (g(t)) for (let e of t.split(";")) {
			let t = e.slice(0, e.indexOf(":")).trim();
			n[t] ?? Pa(r, t, "");
		}
		else for (let e in t) n[e] ?? Pa(r, e, "");
		for (let i in n) {
			i === "display" && (a = !0);
			let o = n[i];
			o == null ? Pa(r, i, "") : Ra(e, i, !g(t) && t ? t[i] : void 0, o) || Pa(r, i, o);
		}
	} else if (i) {
		if (t !== n) {
			let e = r[Aa];
			e && (n += ";" + e), r.cssText = n, a = ja.test(n);
		}
	} else t && e.removeAttribute("style");
	Oa in e && (e[Oa] = a ? r.display : "", e[ka] && (r.display = "none"));
}
var Na = /\s*!important$/;
function Pa(e, t, n) {
	if (d(n)) n.forEach((n) => Pa(e, t, n));
	else if (n ??= "", t.startsWith("--")) e.setProperty(t, n);
	else {
		let r = La(e, t);
		Na.test(n) ? e.setProperty(E(r), n.replace(Na, ""), "important") : e[r] = n;
	}
}
var Fa = [
	"Webkit",
	"Moz",
	"ms"
], Ia = {};
function La(e, t) {
	let n = Ia[t];
	if (n) return n;
	let r = T(t);
	if (r !== "filter" && r in e) return Ia[t] = r;
	r = ie(r);
	for (let n = 0; n < Fa.length; n++) {
		let i = Fa[n] + r;
		if (i in e) return Ia[t] = i;
	}
	return t;
}
function Ra(e, t, n, r) {
	return e.tagName === "TEXTAREA" && (t === "width" || t === "height") && g(r) && n === r;
}
var za = "http://www.w3.org/1999/xlink";
function Ba(e, t, n, r, i, a = ge(t)) {
	r && t.startsWith("xlink:") ? n == null ? e.removeAttributeNS(za, t.slice(6, t.length)) : e.setAttributeNS(za, t, n) : n == null || a && !_e(n) ? e.removeAttribute(t) : e.setAttribute(t, a ? "" : _(n) ? String(n) : n);
}
function Va(e, t, n, r, i) {
	if (t === "innerHTML" || t === "textContent") {
		n != null && (e[t] = t === "innerHTML" ? ba(n) : n);
		return;
	}
	let a = e.tagName;
	if (t === "value" && a !== "PROGRESS" && !a.includes("-")) {
		let r = a === "OPTION" ? e.getAttribute("value") || "" : e.value, i = n == null ? e.type === "checkbox" ? "on" : "" : String(n);
		(r !== i || !("_value" in e)) && (e.value = i), n ?? e.removeAttribute(t), e._value = n;
		return;
	}
	let o = !1;
	if (n === "" || n == null) {
		let r = typeof e[t];
		r === "boolean" ? n = _e(n) : n == null && r === "string" ? (n = "", o = !0) : r === "number" && (n = 0, o = !0);
	}
	try {
		e[t] = n;
	} catch {}
	o && e.removeAttribute(i || t);
}
function Ha(e, t, n, r) {
	e.addEventListener(t, n, r);
}
function Ua(e, t, n, r) {
	e.removeEventListener(t, n, r);
}
var Wa = /* @__PURE__ */ Symbol("_vei");
function Ga(e, t, n, r, i = null) {
	let a = e[Wa] || (e[Wa] = {}), o = a[t];
	if (r && o) o.value = r;
	else {
		let [n, s] = Ja(t);
		r ? Ha(e, n, a[t] = Qa(r, i), s) : o && (Ua(e, n, o, s), a[t] = void 0);
	}
}
var Ka = /(Once|Passive|Capture)$/, qa = /^on:?(?:Once|Passive|Capture)$/;
function Ja(e) {
	let t, n;
	for (; (n = e.match(Ka)) && !qa.test(e);) t ||= {}, e = e.slice(0, e.length - n[1].length), t[n[1].toLowerCase()] = !0;
	return [e[2] === ":" ? e.slice(3) : E(e.slice(2)), t];
}
var Ya = 0, Xa = /* @__PURE__ */ Promise.resolve(), Za = () => Ya ||= (Xa.then(() => Ya = 0), Date.now());
function Qa(e, t) {
	let n = (e) => {
		if (!e._vts) e._vts = Date.now();
		else if (e._vts <= n.attached) return;
		let r = n.value;
		if (d(r)) {
			let n = e.stopImmediatePropagation;
			e.stopImmediatePropagation = () => {
				n.call(e), e._stopped = !0;
			};
			let i = r.slice(), a = [e];
			for (let n = 0; n < i.length && !e._stopped; n++) {
				let e = i[n];
				e && z(e, t, 5, a);
			}
		} else z(r, t, 5, [e]);
	};
	return n.value = e, n.attached = Za(), n;
}
var $a = (e) => e.charCodeAt(0) === 111 && e.charCodeAt(1) === 110 && e.charCodeAt(2) > 96 && e.charCodeAt(2) < 123, eo = (e, t, n, r, i, s) => {
	let c = i === "svg";
	t === "class" ? Da(e, r, c) : t === "style" ? Ma(e, n, r) : a(t) ? o(t) || Ga(e, t, n, r, s) : (t[0] === "." ? (t = t.slice(1), !0) : t[0] === "^" ? (t = t.slice(1), !1) : to(e, t, r, c)) ? (Va(e, t, r), !e.tagName.includes("-") && (t === "value" || t === "checked" || t === "selected") && Ba(e, t, r, c, s, t !== "value")) : e._isVueCE && (no(e, t) || e._def.__asyncLoader && (/[A-Z]/.test(t) || !g(r))) ? Va(e, T(t), r, s, t) : (t === "true-value" ? e._trueValue = r : t === "false-value" && (e._falseValue = r), Ba(e, t, r, c));
};
function to(e, t, n, r) {
	if (r) return !!(t === "innerHTML" || t === "textContent" || t in e && $a(t) && h(n));
	if (t === "spellcheck" || t === "draggable" || t === "translate" || t === "autocorrect" || t === "sandbox" && e.tagName === "IFRAME" || t === "form" || t === "list" && e.tagName === "INPUT" || t === "type" && e.tagName === "TEXTAREA") return !1;
	if (t === "width" || t === "height") {
		let t = e.tagName;
		if (t === "IMG" || t === "VIDEO" || t === "CANVAS" || t === "SOURCE") return !1;
	}
	return $a(t) && g(n) ? !1 : t in e;
}
function no(e, t) {
	let n = e._def.props;
	if (!n) return !1;
	let r = T(t);
	return Array.isArray(n) ? n.some((e) => T(e) === r) : Object.keys(n).some((e) => T(e) === r);
}
var ro = [
	"ctrl",
	"shift",
	"alt",
	"meta"
], io = {
	stop: (e) => e.stopPropagation(),
	prevent: (e) => e.preventDefault(),
	self: (e) => e.target !== e.currentTarget,
	ctrl: (e) => !e.ctrlKey,
	shift: (e) => !e.shiftKey,
	alt: (e) => !e.altKey,
	meta: (e) => !e.metaKey,
	left: (e) => "button" in e && e.button !== 0,
	middle: (e) => "button" in e && e.button !== 1,
	right: (e) => "button" in e && e.button !== 2,
	exact: (e, t) => ro.some((n) => e[`${n}Key`] && !t.includes(n))
}, ao = (e, t) => {
	if (!e) return e;
	let n = e._withMods ||= {}, r = t.join(".");
	return n[r] || (n[r] = ((n, ...r) => {
		for (let e = 0; e < t.length; e++) {
			let r = io[t[e]];
			if (r && r(n, t)) return;
		}
		return e(n, ...r);
	}));
}, oo = /* @__PURE__ */ s({ patchProp: eo }, Ta), so;
function co() {
	return so ||= vi(oo);
}
var lo = ((...e) => {
	let t = co().createApp(...e), { mount: n } = t;
	return t.mount = (e) => {
		let r = fo(e);
		if (!r) return;
		let i = t._component;
		!h(i) && !i.render && !i.template && (i.template = r.innerHTML), r.nodeType === 1 && (r.textContent = "");
		let a = n(r, !1, uo(r));
		return r instanceof Element && (r.removeAttribute("v-cloak"), r.setAttribute("data-v-app", "")), a;
	}, t;
});
function uo(e) {
	if (e instanceof SVGElement) return "svg";
	if (typeof MathMLElement == "function" && e instanceof MathMLElement) return "mathml";
}
function fo(e) {
	return g(e) ? document.querySelector(e) : e;
}
//#endregion
//#region src/music/api.ts
async function po(e) {
	if (e.ok) return await e.json();
	let t = e.statusText || "Request failed";
	try {
		let n = await e.json();
		if (typeof n.detail == "string") t = n.detail;
		else if (n.detail && typeof n.detail == "object") {
			let e = n.detail;
			t = e.message || e.detail || t;
		}
	} catch {}
	throw Error(t);
}
async function mo(e) {
	return po(await fetch(e, {
		headers: { Accept: "application/json" },
		credentials: "same-origin"
	}));
}
async function ho(e, t, n) {
	return po(await fetch(e, {
		method: "POST",
		credentials: "same-origin",
		headers: {
			Accept: "application/json",
			"Content-Type": "application/json"
		},
		body: JSON.stringify({
			action: t,
			payload: n
		})
	}));
}
//#endregion
//#region src/music/components/DynamicField.vue?vue&type=script&setup=true&lang.ts
var go = ["checked", "disabled"], _o = { key: 0 }, vo = { class: "tm-option-grid" }, yo = [
	"checked",
	"disabled",
	"onChange"
], bo = { key: 0 }, xo = ["value", "disabled"], So = ["value"], Co = { key: 0 }, wo = { class: "tm-range-row" }, To = [
	"value",
	"min",
	"max",
	"step",
	"disabled"
], Eo = [
	"value",
	"placeholder",
	"required",
	"disabled"
], Do = [
	"type",
	"value",
	"placeholder",
	"required",
	"disabled",
	"min",
	"max",
	"step"
], Oo = { key: 2 }, ko = /* @__PURE__ */ Jn({
	__name: "DynamicField",
	props: {
		field: {},
		modelValue: {},
		compact: { type: Boolean }
	},
	emits: ["update:modelValue"],
	setup(e, { emit: t }) {
		let n = e, r = t, i = $(() => String(n.field.type || "text").toLowerCase()), a = $(() => !!(n.field.disabled || n.field.read_only)), o = $(() => String(n.modelValue ?? "")), s = $(() => Number(n.modelValue ?? 0)), c = $(() => new Set((Array.isArray(n.modelValue) ? n.modelValue : [n.modelValue]).map((e) => String(e ?? "")).filter(Boolean)));
		function l(e) {
			return String(e && typeof e == "object" ? e.value ?? e.id ?? e.key ?? e.label ?? "" : e ?? "");
		}
		function u(e) {
			return String(e && typeof e == "object" ? e.label ?? l(e) : e ?? "");
		}
		function d(e) {
			let t = e.target;
			i.value === "checkbox" ? r("update:modelValue", t.checked) : i.value === "number" || i.value === "range" ? r("update:modelValue", Number(t.value)) : r("update:modelValue", t.value);
		}
		function f(e, t) {
			let n = new Set(c.value);
			t ? n.add(e) : n.delete(e), r("update:modelValue", Array.from(n));
		}
		return (t, n) => i.value === "checkbox" ? (K(), q("label", {
			key: 0,
			class: k(["tm-field tm-checkbox", { compact: e.compact }])
		}, [J("input", {
			type: "checkbox",
			checked: !!e.modelValue,
			disabled: a.value,
			onChange: d
		}, null, 40, go), J("span", null, [J("strong", null, A(e.field.label || e.field.key), 1), e.field.description ? (K(), q("small", _o, A(e.field.description), 1)) : Y("", !0)])], 2)) : i.value === "multiselect" ? (K(), q("fieldset", {
			key: 1,
			class: k(["tm-field tm-multiselect", { compact: e.compact }])
		}, [
			J("legend", null, A(e.field.label || e.field.key), 1),
			J("div", vo, [(K(!0), q(W, null, V(e.field.options || [], (e) => (K(), q("label", {
				key: l(e),
				class: "tm-option"
			}, [J("input", {
				type: "checkbox",
				checked: c.value.has(l(e)),
				disabled: a.value || !l(e),
				onChange: (t) => f(l(e), t.target.checked)
			}, null, 40, yo), J("span", null, A(u(e)), 1)]))), 128))]),
			e.field.description ? (K(), q("small", bo, A(e.field.description), 1)) : Y("", !0)
		], 2)) : i.value === "select" ? (K(), q("label", {
			key: 2,
			class: k(["tm-field", { compact: e.compact }])
		}, [
			J("span", null, A(e.field.label || e.field.key), 1),
			J("select", {
				value: o.value,
				disabled: a.value,
				onChange: d
			}, [(K(!0), q(W, null, V(e.field.options || [], (e) => (K(), q("option", {
				key: l(e),
				value: l(e)
			}, A(u(e)), 9, So))), 128))], 40, xo),
			e.field.description ? (K(), q("small", Co, A(e.field.description), 1)) : Y("", !0)
		], 2)) : i.value === "range" ? (K(), q("label", {
			key: 3,
			class: k(["tm-field tm-range", { compact: e.compact }])
		}, [J("span", null, A(e.field.label || e.field.key), 1), J("div", wo, [J("input", {
			type: "range",
			value: s.value,
			min: e.field.min ?? 0,
			max: e.field.max ?? 100,
			step: e.field.step ?? 1,
			disabled: a.value,
			onInput: d
		}, null, 40, To), J("output", null, A(s.value) + A(e.field.suffix || ""), 1)])], 2)) : (K(), q("label", {
			key: 4,
			class: k(["tm-field", { compact: e.compact }])
		}, [
			J("span", null, A(e.field.label || e.field.key), 1),
			i.value === "textarea" || i.value === "multiline" ? (K(), q("textarea", {
				key: 0,
				value: o.value,
				placeholder: e.field.placeholder,
				required: e.field.required,
				disabled: a.value,
				onInput: d
			}, null, 40, Eo)) : (K(), q("input", {
				key: 1,
				type: i.value === "password" ? "password" : i.value === "number" ? "number" : "text",
				value: e.modelValue,
				placeholder: e.field.placeholder,
				required: e.field.required,
				disabled: a.value,
				min: e.field.min,
				max: e.field.max,
				step: e.field.step,
				onInput: d
			}, null, 40, Do)),
			e.field.description ? (K(), q("small", Oo, A(e.field.description), 1)) : Y("", !0)
		], 2));
	}
}), Ao = { class: "tm-library" }, jo = {
	class: "tm-subtabs",
	"aria-label": "Browse music library"
}, Mo = ["onClick"], No = { class: "tm-search-controls" }, Po = ["disabled"], Fo = {
	key: 0,
	class: "tm-library-grid"
}, Io = { class: "tm-library-art" }, Lo = ["src", "alt"], Ro = {
	key: 1,
	"aria-hidden": "true"
}, zo = [
	"disabled",
	"aria-label",
	"onClick"
], Bo = { class: "tm-library-copy" }, Vo = ["title"], Ho = {
	key: 1,
	class: "tm-empty"
}, Uo = {
	key: 2,
	class: "tm-pagination",
	"aria-label": "Library pages"
}, Wo = ["disabled"], Go = ["disabled"], Ko = /* @__PURE__ */ Jn({
	__name: "LibraryBrowser",
	props: {
		groups: {},
		items: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ Bt(t.groups[0]?.key || "search"), r = /* @__PURE__ */ Bt({}), i = /* @__PURE__ */ Bt({});
		On(() => t.groups, (e) => {
			e.some((e) => e.key === n.value) || (n.value = e[0]?.key || "search");
		}, { deep: !0 });
		let a = $(() => t.groups.find((e) => e.key === n.value)), o = $(() => {
			let e = a.value?.item_group || a.value?.key;
			return t.items.filter((t) => t.group === e);
		}), s = $(() => o.value[0]), c = $(() => Math.max(0, Number(a.value?.page_size || 0))), l = $(() => Math.max(1, r.value[n.value] || 1)), u = $(() => c.value ? Math.max(1, Math.ceil(o.value.length / c.value)) : 1), d = $(() => {
			if (!c.value) return o.value;
			let e = (Math.min(l.value, u.value) - 1) * c.value;
			return o.value.slice(e, e + c.value);
		});
		On(s, (e) => {
			if (!e) return;
			let t = { ...i.value };
			for (let n of e.fields || []) n.key in t || (t[n.key] = n.value);
			i.value = t;
		}, { immediate: !0 });
		function f(e) {
			r.value = {
				...r.value,
				[n.value]: Math.max(1, Math.min(e, u.value))
			};
		}
		function p(e, t) {
			i.value = {
				...i.value,
				[e.key]: t
			};
		}
		async function m() {
			let e = s.value;
			e?.run_action && await t.run(e.run_action, {
				id: e.id,
				values: i.value
			}, `item:${e.id}`);
		}
		async function h(e) {
			e.run_action && await t.run(e.run_action, {
				id: e.id,
				values: {}
			}, `item:${e.id}`);
		}
		return (t, r) => (K(), q("section", Ao, [J("nav", jo, [(K(!0), q(W, null, V(e.groups, (e) => (K(), q("button", {
			key: e.key,
			type: "button",
			class: k({ active: n.value === e.key }),
			onClick: (t) => n.value = e.key
		}, A(e.label || e.key), 11, Mo))), 128))]), a.value?.key === "search" && s.value ? (K(), q("form", {
			key: 0,
			class: "tm-search",
			onSubmit: ao(m, ["prevent"])
		}, [J("div", null, [
			r[2] ||= J("div", { class: "tm-eyebrow" }, "Search across your connected library", -1),
			J("h3", null, A(s.value.title || "Find music"), 1),
			J("p", null, A(s.value.subtitle), 1)
		]), J("div", No, [(K(!0), q(W, null, V(s.value.fields || [], (e) => (K(), Ri(ko, {
			key: e.key,
			field: e,
			"model-value": i.value[e.key],
			compact: "",
			"onUpdate:modelValue": (t) => p(e, t)
		}, null, 8, [
			"field",
			"model-value",
			"onUpdate:modelValue"
		]))), 128)), J("button", {
			type: "submit",
			class: "tm-button primary",
			disabled: e.busy(`item:${s.value.id}`)
		}, A(s.value.run_label || "Play Search"), 9, Po)])], 32)) : (K(), q(W, { key: 1 }, [d.value.length ? (K(), q("div", Fo, [(K(!0), q(W, null, V(d.value, (t) => (K(), q("article", {
			key: t.id,
			class: "tm-library-card"
		}, [J("div", Io, [t.hero_image_src ? (K(), q("img", {
			key: 0,
			src: t.hero_image_src,
			alt: t.hero_image_alt || "",
			loading: "lazy"
		}, null, 8, Lo)) : (K(), q("span", Ro, "♫")), t.run_action ? (K(), q("button", {
			key: 2,
			type: "button",
			disabled: e.busy(`item:${t.id}`),
			"aria-label": `${t.run_label || "Play"} ${t.title || ""}`,
			onClick: (e) => h(t)
		}, " ▶ ", 8, zo)) : Y("", !0)]), J("div", Bo, [J("strong", { title: t.title }, A(t.title || "Untitled"), 9, Vo), J("small", null, A(t.subtitle), 1)])]))), 128))])) : (K(), q("div", Ho, A(a.value?.empty_message || "Nothing is available here yet."), 1)), u.value > 1 ? (K(), q("div", Uo, [
			J("button", {
				type: "button",
				disabled: l.value <= 1,
				onClick: r[0] ||= (e) => f(l.value - 1)
			}, "Previous", 8, Wo),
			J("span", null, "Page " + A(Math.min(l.value, u.value)) + " of " + A(u.value), 1),
			J("button", {
				type: "button",
				disabled: l.value >= u.value,
				onClick: r[1] ||= (e) => f(l.value + 1)
			}, "Next", 8, Go)
		])) : Y("", !0)], 64))]));
	}
}), qo = {
	class: "tm-queue",
	open: ""
}, Jo = ["checked", "disabled"], Yo = {
	key: 0,
	class: "tm-track-scroll",
	role: "listbox",
	"aria-label": "Current track list"
}, Xo = [
	"disabled",
	"aria-current",
	"title",
	"onDblclick"
], Zo = { class: "tm-track-position" }, Qo = { class: "tm-track-copy" }, $o = { class: "tm-track-duration" }, es = {
	key: 1,
	class: "tm-empty compact"
}, ts = /* @__PURE__ */ Jn({
	__name: "TrackList",
	props: {
		item: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e;
		async function n(e) {
			!t.item.track_list_action || !e.id || await t.run(t.item.track_list_action, {
				id: e.id,
				values: {}
			}, `track:${e.id}`);
		}
		async function r(e) {
			let n = e.target;
			t.item.track_list_shuffle_action && (await t.run(t.item.track_list_shuffle_action, {
				id: t.item.id,
				values: { shuffle: n.checked }
			}, "shuffle") || (n.checked = !n.checked));
		}
		return (t, i) => (K(), q("details", qo, [J("summary", null, [J("span", null, [J("strong", null, A(e.item.track_list_label || "Current Track List"), 1), J("small", null, A(e.item.track_list?.length || 0) + " tracks", 1)]), J("label", {
			class: "tm-shuffle",
			onClick: i[0] ||= ao(() => {}, ["stop"])
		}, [J("input", {
			type: "checkbox",
			checked: !!e.item.track_list_shuffle,
			disabled: e.busy("shuffle"),
			onChange: r
		}, null, 40, Jo), i[1] ||= qi(" Shuffle ", -1)])]), e.item.track_list?.length ? (K(), q("div", Yo, [(K(!0), q(W, null, V(e.item.track_list, (t) => (K(), q("button", {
			key: t.id || t.position,
			type: "button",
			class: k(["tm-track", {
				active: t.active,
				pending: e.busy(`track:${t.id}`)
			}]),
			disabled: e.busy(`track:${t.id}`),
			"aria-current": t.active ? "true" : void 0,
			title: `Double-click to play ${t.title || "this track"}`,
			onDblclick: (e) => n(t)
		}, [
			J("span", Zo, A(t.active ? "▶" : t.position), 1),
			J("span", Qo, [J("strong", null, A(t.title || "Untitled"), 1), J("small", null, A([t.artist, t.album].filter(Boolean).join(" · ") || "Unknown artist"), 1)]),
			J("span", $o, A(t.duration || ""), 1)
		], 42, Xo))), 128))])) : (K(), q("div", es, "Play an album, artist, genre, or search to create a track list."))]));
	}
}), ns = {
	class: "tm-player",
	"aria-label": "Music player"
}, rs = { class: "tm-player-main" }, is = { class: "tm-art-wrap" }, as = ["src", "alt"], os = {
	key: 1,
	class: "tm-art tm-art-placeholder",
	"aria-hidden": "true"
}, ss = { class: "tm-now-playing" }, cs = {
	key: 0,
	class: "tm-badges"
}, ls = { class: "tm-player-controls" }, us = {
	class: "tm-transport",
	"aria-label": "Playback controls"
}, ds = [
	"disabled",
	"aria-label",
	"title",
	"onClick"
], fs = { class: "tm-volume-speakers" }, ps = ["aria-label"], ms = {
	key: 0,
	class: "tm-player-facts"
}, hs = {
	class: "tm-modal",
	role: "dialog",
	"aria-modal": "true",
	"aria-labelledby": "tm-speaker-title"
}, gs = { id: "tm-speaker-title" }, _s = { class: "tm-modal-body" }, vs = ["disabled"], ys = /* @__PURE__ */ Jn({
	__name: "MusicPlayer",
	props: {
		item: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ Bt(!1), r = /* @__PURE__ */ Bt(75), i = /* @__PURE__ */ Bt({}), a = $(() => t.item.fields?.find((e) => e.key === "volume_percent")), o = $(() => t.item.popup_fields || []);
		function s(e) {
			return Array.isArray(e) ? e.map((e) => e && typeof e == "object" ? { ...e } : e) : e && typeof e == "object" ? { ...e } : e;
		}
		On(a, (e) => {
			e && (r.value = Number(e.value ?? 75));
		}, { immediate: !0 }), On(o, (e) => {
			i.value = Object.fromEntries(e.map((e) => [e.key, s(e.value)]));
		}, { immediate: !0 });
		function c(e) {
			return e.endsWith("_play") ? "primary" : e.endsWith("_stop") ? "stop" : "";
		}
		function l(e, t) {
			return e.endsWith("_previous") ? "⏮" : e.endsWith("_play") ? "▶" : e.endsWith("_stop") ? "■" : e.endsWith("_next") ? "⏭" : t;
		}
		async function u(e) {
			await t.run(e, {
				id: t.item.id,
				values: { volume_percent: r.value }
			}, "transport");
		}
		async function d() {
			let e = a.value;
			e?.action && await t.run(e.action, {
				id: t.item.id,
				values: { volume_percent: r.value }
			}, "volume");
		}
		async function f() {
			t.item.save_action && await t.run(t.item.save_action, {
				id: t.item.id,
				values: i.value
			}, "speakers") && (n.value = !1);
		}
		function p(e, t) {
			i.value = {
				...i.value,
				[e.key]: t
			};
		}
		return (t, s) => (K(), q("section", ns, [
			J("div", rs, [
				J("div", is, [e.item.hero_image_src ? (K(), q("img", {
					key: 0,
					class: "tm-art",
					src: e.item.hero_image_src,
					alt: e.item.hero_image_alt || ""
				}, null, 8, as)) : (K(), q("div", os, "♫"))]),
				J("div", ss, [
					s[5] ||= J("div", { class: "tm-eyebrow" }, "Now playing", -1),
					J("h2", null, A(e.item.title || "Music Player"), 1),
					J("p", null, A(e.item.subtitle || e.item.detail), 1),
					e.item.hero_badges?.length ? (K(), q("div", cs, [(K(!0), q(W, null, V(e.item.hero_badges, (e) => (K(), q("span", {
						key: e.label,
						class: k(`tone-${e.tone || "muted"}`)
					}, A(e.label), 3))), 128))])) : Y("", !0)
				]),
				J("div", ls, [J("div", us, [(K(!0), q(W, null, V(e.item.actions || [], (t) => (K(), q("button", {
					key: t.action,
					type: "button",
					class: k(c(t.action)),
					disabled: e.busy("transport"),
					"aria-label": t.aria_label || t.label,
					title: t.tooltip || t.label,
					onClick: (e) => u(t.action)
				}, A(l(t.action, t.label || "Run")), 11, ds))), 128))]), J("div", fs, [a.value ? (K(), Ri(ko, {
					key: 0,
					field: a.value,
					"model-value": r.value,
					compact: "",
					"onUpdate:modelValue": s[0] ||= (e) => r.value = Number(e),
					onChange: d
				}, null, 8, ["field", "model-value"])) : Y("", !0), J("button", {
					type: "button",
					class: "tm-speaker-button",
					"aria-label": e.item.settings_aria_label || "Choose speakers and players",
					title: "Choose speakers and players",
					onClick: s[1] ||= (e) => n.value = !0
				}, [...s[6] ||= [J("span", { "aria-hidden": "true" }, "🔊", -1), J("span", { class: "tm-speaker-label" }, "Players", -1)]], 8, ps)])])
			]),
			e.item.summary_rows?.length ? (K(), q("div", ms, [(K(!0), q(W, null, V(e.item.summary_rows, (e) => (K(), q("div", { key: e.label }, [J("span", null, A(e.label), 1), J("strong", null, A(e.value || "—"), 1)]))), 128))])) : Y("", !0),
			Ui(ts, {
				item: e.item,
				busy: e.busy,
				run: e.run
			}, null, 8, [
				"item",
				"busy",
				"run"
			]),
			(K(), Ri(Un, { to: "body" }, [n.value ? (K(), q("div", {
				key: 0,
				class: "tm-modal-backdrop",
				onClick: s[4] ||= ao((e) => n.value = !1, ["self"])
			}, [J("section", hs, [
				J("header", null, [J("div", null, [s[7] ||= J("div", { class: "tm-eyebrow" }, "Playback destination", -1), J("h3", gs, A(e.item.settings_title || "Choose Speakers & Players"), 1)]), J("button", {
					type: "button",
					class: "tm-close",
					"aria-label": "Close",
					onClick: s[2] ||= (e) => n.value = !1
				}, "×")]),
				J("div", _s, [(K(!0), q(W, null, V(o.value, (e) => (K(), Ri(ko, {
					key: e.key,
					field: e,
					"model-value": i.value[e.key],
					"onUpdate:modelValue": (t) => p(e, t)
				}, null, 8, [
					"field",
					"model-value",
					"onUpdate:modelValue"
				]))), 128))]),
				J("footer", null, [J("button", {
					type: "button",
					class: "tm-button secondary",
					onClick: s[3] ||= (e) => n.value = !1
				}, "Cancel"), J("button", {
					type: "button",
					class: "tm-button primary",
					disabled: e.busy("speakers"),
					onClick: f
				}, " Set players ", 8, vs)])
			])])) : Y("", !0)]))
		]));
	}
}), bs = { class: "tm-settings-card" }, xs = {
	key: 0,
	class: "tm-badges"
}, Ss = {
	key: 0,
	class: "tm-card-detail"
}, Cs = {
	key: 1,
	class: "tm-settings-fields"
}, ws = { class: "tm-form-grid" }, Ts = {
	key: 2,
	class: "tm-form-grid"
}, Es = { key: 3 }, Ds = ["disabled", "onClick"], Os = ["disabled"], ks = /* @__PURE__ */ Jn({
	__name: "SettingsCard",
	props: {
		item: {},
		busy: { type: Function },
		run: { type: Function }
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ jt({}), r = /* @__PURE__ */ new Set();
		function i(e) {
			return Array.isArray(e) ? e.map((e) => e && typeof e == "object" ? { ...e } : e) : e && typeof e == "object" ? { ...e } : e;
		}
		On(() => t.item.fields, (e) => {
			for (let t of e || []) r.has(t.key) || (n[t.key] = i(t.value));
		}, {
			immediate: !0,
			deep: !0
		});
		function a(e, t) {
			n[e.key] = t, r.add(e.key);
		}
		async function o() {
			t.item.save_action && await t.run(t.item.save_action, {
				id: t.item.id,
				values: { ...n }
			}, `item:${t.item.id}:save`) && r.clear();
		}
		async function s(e) {
			e.confirm && !window.confirm(e.confirm) || await t.run(e.action, {
				id: t.item.id,
				values: { ...n }
			}, `item:${t.item.id}:${e.action}`) && r.clear();
		}
		return (t, r) => (K(), q("article", bs, [
			J("header", null, [J("div", null, [J("h3", null, A(e.item.title || e.item.id), 1), J("p", null, A(e.item.subtitle), 1)]), e.item.hero_badges?.length ? (K(), q("div", xs, [(K(!0), q(W, null, V(e.item.hero_badges, (e) => (K(), q("span", {
				key: e.label,
				class: k(`tone-${e.tone || "muted"}`)
			}, A(e.label), 3))), 128))])) : Y("", !0)]),
			e.item.detail ? (K(), q("p", Ss, A(e.item.detail), 1)) : Y("", !0),
			e.item.fields_dropdown && e.item.fields?.length ? (K(), q("details", Cs, [r[0] ||= J("summary", null, "Connection settings", -1), J("div", ws, [(K(!0), q(W, null, V(e.item.fields, (e) => (K(), Ri(ko, {
				key: e.key,
				field: e,
				"model-value": n[e.key],
				"onUpdate:modelValue": (t) => a(e, t)
			}, null, 8, [
				"field",
				"model-value",
				"onUpdate:modelValue"
			]))), 128))])])) : e.item.fields?.length ? (K(), q("div", Ts, [(K(!0), q(W, null, V(e.item.fields, (e) => (K(), Ri(ko, {
				key: e.key,
				field: e,
				"model-value": n[e.key],
				"onUpdate:modelValue": (t) => a(e, t)
			}, null, 8, [
				"field",
				"model-value",
				"onUpdate:modelValue"
			]))), 128))])) : Y("", !0),
			e.item.actions?.length || e.item.save_action ? (K(), q("footer", Es, [(K(!0), q(W, null, V(e.item.actions || [], (t) => (K(), q("button", {
				key: t.action,
				type: "button",
				class: k(["tm-button", t.tone === "danger" ? "danger" : t.action.includes("activate") ? "primary" : "secondary"]),
				disabled: e.busy(`item:${e.item.id}:${t.action}`),
				onClick: (e) => s(t)
			}, A(t.label || "Run"), 11, Ds))), 128)), e.item.save_action ? (K(), q("button", {
				key: 0,
				type: "button",
				class: "tm-button primary",
				disabled: e.busy(`item:${e.item.id}:save`),
				onClick: o
			}, A(e.item.save_label || "Save"), 9, Os)) : Y("", !0)])) : Y("", !0)
		]));
	}
}), As = { class: "tater-music-core" }, js = {
	key: 0,
	class: "tm-error"
}, Ms = { class: "tm-page-heading" }, Ns = ["title"], Ps = {
	key: 0,
	class: "tm-stats",
	"aria-label": "Music library status"
}, Fs = {
	class: "tm-tabs",
	"aria-label": "Music Core sections"
}, Is = ["onClick"], Ls = {
	key: 0,
	class: "tm-empty"
}, Rs = {
	key: 4,
	class: "tm-error-toast",
	role: "alert"
}, zs = /* @__PURE__ */ Jn({
	__name: "MusicCoreApp",
	props: {
		state: {},
		options: {}
	},
	setup(e) {
		let t = e, n = /* @__PURE__ */ Bt(""), r = /* @__PURE__ */ Bt(/* @__PURE__ */ new Set()), i = /* @__PURE__ */ Bt(""), a = /* @__PURE__ */ Bt("connecting"), o = null, s = 0, c = $(() => t.state.payload || {}), l = $(() => c.value.ui || {}), u = $(() => l.value.item_forms || []), d = $(() => u.value.find((e) => e.group === "player")), f = $(() => l.value.manager_tabs || []), p = $(() => f.value.find((e) => e.key === n.value) || f.value[0]), m = $(() => {
			let e = p.value;
			return !e || e.source === "grouped_items" ? [] : u.value.filter((t) => !e.item_group || t.group === e.item_group);
		});
		On(f, (e) => {
			if (!e.some((e) => e.key === n.value)) {
				let t = String(l.value.default_tab || "");
				n.value = e.some((e) => e.key === t) ? t : e[0]?.key || "";
			}
		}, {
			immediate: !0,
			deep: !0
		});
		function h(e) {
			return r.value.has(e);
		}
		function g(e, t) {
			let n = new Set(r.value);
			t ? n.add(e) : n.delete(e), r.value = n;
		}
		async function _() {
			t.state.payload = await mo(t.options.tabEndpoint);
		}
		async function v(e, n, r = e) {
			if (!e || h(r)) return !1;
			i.value = "", g(r, !0);
			try {
				return await ho(t.options.actionEndpoint, e, n), await _(), !0;
			} catch (e) {
				return i.value = e instanceof Error ? e.message : String(e || "Music action failed."), !1;
			} finally {
				g(r, !1);
			}
		}
		function y() {
			o && o.close(), s && window.clearTimeout(s), a.value = "connecting", o = new EventSource(t.options.eventsEndpoint), o.addEventListener("core-tab", (e) => {
				try {
					t.state.payload = JSON.parse(e.data), a.value = "live";
				} catch {}
			}), o.addEventListener("open", () => {
				a.value = "live";
			}), o.addEventListener("error", () => {
				a.value = "offline", o?.close(), o = null, s = window.setTimeout(y, 3e3);
			});
		}
		return lr(y), fr(() => {
			s && window.clearTimeout(s), o?.close(), o = null;
		}), (e, t) => (K(), q("main", As, [c.value.error ? (K(), q("div", js, A(c.value.error), 1)) : (K(), q(W, { key: 1 }, [
			J("header", Ms, [J("div", null, [
				t[1] ||= J("div", { class: "tm-eyebrow" }, "Tater Music", -1),
				J("h1", null, A(l.value.title || "Music Core"), 1),
				J("p", null, A(c.value.summary), 1)
			]), J("div", {
				class: k(["tm-live-state", a.value]),
				title: `Music updates: ${a.value}`
			}, [t[2] ||= J("span", null, null, -1), qi(A(a.value === "live" ? "Live" : a.value === "connecting" ? "Connecting" : "Reconnecting"), 1)], 10, Ns)]),
			c.value.stats?.length ? (K(), q("section", Ps, [(K(!0), q(W, null, V(c.value.stats, (e) => (K(), q("div", { key: e.label }, [J("span", null, A(e.label), 1), J("strong", null, A(e.value ?? "—"), 1)]))), 128))])) : Y("", !0),
			d.value ? (K(), Ri(ys, {
				key: 1,
				item: d.value,
				busy: h,
				run: v
			}, null, 8, ["item"])) : Y("", !0),
			J("nav", Fs, [(K(!0), q(W, null, V(f.value, (e) => (K(), q("button", {
				key: e.key,
				type: "button",
				class: k({ active: n.value === e.key }),
				onClick: (t) => n.value = e.key
			}, A(e.label || e.key), 11, Is))), 128))]),
			p.value?.source === "grouped_items" ? (K(), Ri(Ko, {
				key: 2,
				groups: p.value.groups || [],
				items: u.value,
				busy: h,
				run: v
			}, null, 8, ["groups", "items"])) : (K(), q("section", {
				key: 3,
				class: k(["tm-settings-grid", `group-${p.value?.item_group || "all"}`])
			}, [(K(!0), q(W, null, V(m.value, (e) => (K(), Ri(ks, {
				key: e.id,
				item: e,
				busy: h,
				run: v
			}, null, 8, ["item"]))), 128)), m.value.length ? Y("", !0) : (K(), q("div", Ls, A(p.value?.empty_message || c.value.empty_message || "Nothing is available here yet."), 1))], 2)),
			i.value ? (K(), q("div", Rs, [J("span", null, A(i.value), 1), J("button", {
				type: "button",
				"aria-label": "Dismiss",
				onClick: t[0] ||= (e) => i.value = ""
			}, "×")])) : Y("", !0)
		], 64))]));
	}
});
//#endregion
//#region src/entry.ts
function Bs(e, t) {
	let n = /* @__PURE__ */ jt({ payload: t.initialPayload }), r = lo(zs, {
		state: n,
		options: t
	});
	return r.mount(e), {
		update(e) {
			n.payload = e;
		},
		unmount() {
			r.unmount();
		}
	};
}
//#endregion
export { Bs as mountMusicCore };
