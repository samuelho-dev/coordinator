# Coordinator third-party notices and release boundaries

Coordinator's original code is licensed under the root [MIT license](LICENSE), copyright 2026 samuelho-dev/Coordinator contributors. This grant does not relicense dependencies, the Node/Debian image, host-native software, assets, or historical third-party material. The image's `org.opencontainers.image.licenses=MIT` describes Coordinator, not a claim that every image component is MIT.

## Current inspector ownership — 2026-10-19

The implemented inspector cutover changes the shipped runtime, not the completeness of the legal review. The [package manifest](package.json) and [Dockerfile](Dockerfile) are the current assembly definitions; the [README](README.md) describes the product, source and native host setup, and the [chart README](deploy/helm/coordinator/README.md) covers deployment. This update is a source-based ownership correction, **not a fresh license audit, resolved-image inventory, or redistribution certification**.

| Component | Current declared input / ownership | Notice obligation / boundary |
| --- | --- | --- |
| Effect | Production dependencies `effect` and `@effect/platform-node` 4.0.3 | Retain matching upstream MIT license/copyright notices, including transitive dependencies; metadata alone is not sufficient. |
| MCP client | Production dependency `@modelcontextprotocol/client` 2.3.1 | The previously inspected LICENSE describes a mixed-license transition: new code/specification and consented contributions Apache-2.0, unconsented legacy contributions MIT, non-specification documentation CC-BY-4.0. Preserve the complete applicable LICENSE/NOTICE/attributions; Apache-2.0 metadata is not a homogeneous-license certification. |
| Node / operating system | Final image `node:24.21.0-bookworm-slim`, Node 24.21.0 / Debian bookworm | Preserve base-image notices and applicable `/usr/share/doc/*/copyright`; system packages have individual licenses, including copyleft terms. Versioned inputs do not establish bit-for-bit reproducibility. |
| Build/development tools | pnpm 11.9.0 in build/dependency stages; TypeScript, esbuild, Effect tooling, Vitest and native SDKs are development inputs | Final assembly copies compiled `dist`, the manifest/notices and production-installed `node_modules`, not `integrations` assets; assess actual emitted/bundled code and transitive/image contents before redistribution, not just dependency labels. |
| Pi / OMP native hosts | Manifest development inputs Pi 1.0.4 and OMP coding-agent/TUI 18.6.1; installed native hosts supply actual APIs | Host SDKs are not direct production dependencies or an inspector-owned SDK-session runtime. Linux smoke also exercised compiled OMP 18.11.3, not a claim that this version is bundled. Operators own their host installation; any separately redistributed host/native closure requires its matching upstream notices and review. |
| Hermes native host | Upstream supplies Hermes itself; Coordinator's shared TypeScript CLI consumes native shell-hook envelopes | Coordinator ships no Python plugin, plugin manifest or Python runtime dependency. Generated hook/MCP configuration refers to the existing CLI; any separately redistributed Hermes runtime requires its own upstream notices and review. |

The current Dockerfile does **not** download/copy standalone OMP or collect OMP notices into `/usr/local/share/coordinator/third-party/`; it adds no PostgreSQL client/backup tooling or application database. `tsx`, `undici` and `ws` are no longer direct runtime dependencies. Their absence from the direct manifest is not proof of absence from every transitive package or base-image byte. No Pi/OMP/native binary closure is asserted to be shipped by the current inspector image.

## npm plugin bundle notices — candidate 0.3.0-beta.1

The OMP extension embeds runtime dependencies to support its native compatibility loader; Pi keeps those dependencies external. Native Pi/OMP SDK implementations are not part of this inventory. The entries below were traced in the existing emitted OMP artifact and matching package distributions. Reconcile them against the newly built, accepted tarball before publication; this source review is not a completed image, history, or legal certification.

| Embedded component | Version | Matching notice source |
| --- | --- | --- |
| `effect`, `@effect/platform-node`, `@effect/platform-node-shared` | 4.0.3 | Matching installed `LICENSE` files; [Effect package](https://registry.npmjs.org/effect/4.0.3) |
| `@modelcontextprotocol/client`, `@modelcontextprotocol/core` | 2.3.1 | Matching installed `LICENSE` files; [client package](https://registry.npmjs.org/@modelcontextprotocol%2fclient/2.3.1) |
| `zod` | 4.6.5 | Matching installed `LICENSE`; [package](https://registry.npmjs.org/zod/4.6.5) |
| `pkce-challenge` | 5.0.1 | Matching installed `LICENSE`; [package](https://registry.npmjs.org/pkce-challenge/5.0.1) |
| `eventsource-parser` | 3.1.1 | Matching installed `LICENSE`; [package](https://registry.npmjs.org/eventsource-parser/3.1.1) |
| `ajv` (embedded by MCP client) | 8.18.0 | [Registry revision's license](https://raw.githubusercontent.com/ajv-validator/ajv/142ce84b807c4fe66e619c22480a28d0e4bd50fa/LICENSE) |
| `ajv-formats` (embedded by MCP client) | 3.0.1 | [Producer lockfile](https://raw.githubusercontent.com/modelcontextprotocol/typescript-sdk/fcef852f6cdafea07f36cf6fae69b37c8adf85be/pnpm-lock.yaml); [registry revision's license](https://raw.githubusercontent.com/ajv-validator/ajv-formats/1fb3fa7d967f919ff924bb34aef73a2f39ab0de3/LICENSE) |
| `fast-deep-equal` (embedded by MCP client) | 3.1.3 | [Registry revision's license](https://raw.githubusercontent.com/epoberezkin/fast-deep-equal/d807ffc5013e710deb1c63d463a03f729bcd144d/LICENSE) |
| `json-schema-traverse` (embedded by MCP client) | 1.0.0 | [Registry revision's license](https://raw.githubusercontent.com/epoberezkin/json-schema-traverse/6b45983cd76270042cc79527da5c8972f13599ec/LICENSE) |
| `fast-uri` (embedded by MCP client) | 3.1.0 | [Registry revision's license](https://raw.githubusercontent.com/fastify/fast-uri/2d08e68f4068320aa3f9da93a6f150e57af50c42/LICENSE) |
| `content-type` (embedded by MCP client) | 1.0.5 | [Registry revision's license](https://raw.githubusercontent.com/jshttp/content-type/6115a4064e4dfd9845241c3f89c233ee2423deeb/LICENSE) |

The complete matching texts follow. Separately installed npm dependencies retain their own notices; the historical image/native-library records below do not assert those components are embedded in this plugin. MCP's licensing-transition statement is preserved rather than replacing its mixed terms with the manifest's single license label.

### Effect family 4.0.3

All three listed Effect packages supply the same notice:

```text
MIT License

Copyright (c) 2023 Effectful Technologies Inc

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### MCP client and core 2.3.1

Both listed MCP packages supply the same licensing-transition statement and license text:

```text
The MCP project is undergoing a licensing transition from the MIT License to the Apache License, Version 2.0 ("Apache-2.0"). All new code and specification contributions to the project are licensed under Apache-2.0. Documentation contributions (excluding specifications) are licensed under CC-BY-4.0.

Contributions for which relicensing consent has been obtained are licensed under Apache-2.0. Contributions made by authors who originally licensed their work under the MIT License and who have not yet granted explicit permission to relicense remain licensed under the MIT License.

No rights beyond those granted by the applicable original license are conveyed for such contributions.

---

                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to the Licensor for inclusion in the Work by the copyright
      owner or by an individual or Legal Entity authorized to submit on behalf
      of the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

---

MIT License

Copyright (c) 2024-2025 Model Context Protocol a Series of LF Projects, LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

---

Creative Commons Attribution 4.0 International (CC-BY-4.0)

Documentation in this project (excluding specifications) is licensed under
CC-BY-4.0. See https://creativecommons.org/licenses/by/4.0/legalcode for
the full license text.
```

### Zod 4.6.5

```text
MIT License

Copyright (c) 2025 Colin McDonnell

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### PKCE challenge 5.0.1

The matching upstream file leaves its copyright holder blank; reproduced as supplied:

```text
MIT License

Copyright (c) 2019 

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Eventsource parser 3.1.1

```text
MIT License

Copyright (c) 2026 Espen Hovlandsdal <espen@hovlandsdal.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Ajv 8.18.0

```text
The MIT License (MIT)

Copyright (c) 2015-2021 Evgeny Poberezkin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Ajv formats 3.0.1

```text
MIT License

Copyright (c) 2020 Evgeny Poberezkin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Fast deep equal 3.1.3 and JSON schema traverse 1.0.0

Both packages supply the same notice:

```text
MIT License

Copyright (c) 2017 Evgeny Poberezkin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

### Fast URI 3.1.0

```text
Copyright (c) 2011-2021, Gary Court until https://github.com/garycourt/uri-js/commit/a1acf730b4bba3f1097c9f52e7d9d3aba8cdcaae
Copyright (c) 2021-present The Fastify team
All rights reserved.

The Fastify team members are listed at https://github.com/fastify/fastify#team.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * The names of any contributors may not be used to endorse or promote
      products derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS AND CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

                                  *   *   *

The complete list of contributors can be found at:
- https://github.com/garycourt/uri-js/graphs/contributors
```

### Content type 1.0.5

```text
(The MIT License)

Copyright (c) 2015 Douglas Christopher Wilson

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```


## Historical bundled inputs (pre-inspector)

The earlier image included standalone `omp-linux-x64` v18.6.1, SHA256 `c92a6846d02984e84f07c6362d18add3783f528f494ffcf1e0f26e594f327463`, at `/usr/local/bin/omp` and matching installed OMP LICENSE/aggregate notices under `/usr/local/share/coordinator/third-party/`. A checksum identified selected bytes, not their complete license closure. Pi 1.0.4 and OMP/OMP natives 18.6.1 were runtime inputs; Pi's inspected root lacked a standalone LICENSE, while OMP supplied LICENSE and `THIRD-PARTY-NOTICES.txt`. Earlier direct runtime inputs also included tsx 4.23.15 / esbuild 0.28.2, undici 8.11.2 and ws 8.22.0, alongside image utilities and PostgreSQL tooling. These records describe older artifacts and their continuing review obligations, **not today's production closure**. Preserve applicable Undici Matteo Collina/contributor and ws Einar Otto Stangvik/Arnout Kazemier/Luigi Pinca notices when redistributing those artifacts.

## Historical observed dependency audit (2026-10-12)

A read-only enumeration at the historical checkout of real, non-symlink package directories in the local pnpm store observed **263 installed package instances**, including stale versions, optional/platform packages, and development dependencies. This is a dated installed-store inventory, **not** the current inspector's resolved production-image dependency closure. Every enumerated instance declared a license in package metadata: 152 MIT, 76 Apache-2.0, 15 BSD-3-Clause, 13 ISC, two BlueOak-1.0.0, and one each 0BSD, Unlicense, MPL-2.0, LGPL-3.0-or-later, and `(MIT OR CC0-1.0)`.

Concrete historical exceptions requiring review for any artifact that contains them (not assertions that the inspector currently ships them):

- `@img/sharp-libvips-linux-x64` 1.3.4 declares **LGPL-3.0-or-later**, and its installed package root has no license file. Its actual README identifies a prebuilt native closure including cairo under MPL-1.1; fribidi, glib, libexif, libheif, librsvg, libvips, pango and proxy-libintl under LGPLv3; and additional permissive, patent and attribution terms. It states LGPLv3 use follows the “any later version” clause of LGPLv2/2.1. Review the accompanying `versions.json`, matching corresponding source, all notices, and any required replacement/relinking rights for the entire closure—not just libvips. Do not treat the containing MIT package as relicensing these libraries.
- `axe-core` 4.13.0 declares **MPL-2.0** and contains LICENSE plus LICENSE-3RD-PARTY.txt. Determine whether it is shipped in the resolved image/standalone executable; if shipped, preserve notices and satisfy source obligations for covered files and modifications.
- `onnxruntime-node` 1.30.0 declares MIT but lacks a root license file in the observed installed package. Audit its native binary/vendor closure separately.
- Several Pi and AWS package instances lack root license files. Metadata declarations do not replace required text/copyright retention; obtain matching-version upstream notices where missing.
- OMP's installed aggregate is a substantial generated notice collection (22,901 lines for the coding-agent package inspected), explicitly says it does not replace package-level licenses, and includes vendored code/assets/native third parties. It was inspected at its introduction and vendored brush/uutils sections; it was **not exhaustively audited** or proven to describe every byte embedded in the downloaded standalone executable.
- The inspected OMP aggregate excludes its own `@img/sharp-libvips-*` 1.2.4 assets because transformers is external, but the historical Coordinator production lock included the optional transformers 4.3 → sharp 0.35.5 → libvips 1.3.4 path. That upstream exclusion was not evidence that Coordinator's then-added native closure was absent or covered by the aggregate. The aggregate also attributes `Silver.ttf` under CC-BY-4.0; retain applicable asset attribution rather than treating all bundled assets as MIT.
- Pi's installed HTML export assets contain copied `marked.min.js` 18.0.5 (MIT header, MarkedJS/Christopher Jeffrey copyrights) and `highlight.min.js` 11.9.0 (BSD-3-Clause header with an incomplete `undefined and other contributors` attribution). These embedded versions differ from the dependency graph's marked 18.0.11/highlight.js 10.7.3. Preserve their own headers and obtain matching complete license/copyright notices; Pi's root MIT metadata is not a substitute for the vendored BSD terms.

The earlier image preserved dependency directories and collected matching OMP first-party/aggregate notices. That historical preservation was not a legal sufficiency certification and is not the current assembly description. Current CI is configured to emit SBOM/BuildKit provenance and release preparation to retrieve them from the exact signed image rather than rebuilding; configuration is not proof that the inspector artifacts were published or audited. Reconcile the actual intended image SBOM and emitted code with applicable notices and sources before redistribution; audit any older standalone/native closure separately if redistributing it.

## Full-source/history boundary

Earlier CI/image configuration produced and copied a full-history `coordinator-source.bundle`; that is a historical observation, not a current artifact requirement. Its runtime consumers were removed, and current CI/Docker builds do not produce or copy it. The later repository-cloning runner was also removed in the inspector cutover; no runner is shipped as current functionality. The image assembly records its source revision through OCI labels and a baked revision argument, while trusted CI/release workflows require source-bound provenance. Labels and source checks alone do not prove provenance or discharge the public source/history audit prerequisite.

At the historical audited checkout, `git rev-list --count HEAD` observed **39 reachable commits**, not a current history count. A read-only historical path listing covered source, tests, scripts, dependency locks, README/research, Docker and workflow files. No complete whole-history secret-content scan, object-by-object confidential-material review, copyright ownership review, or embedded-asset audit was completed. Historical deployment material in Git history includes operator-specific configuration/private infrastructure references; such identifiers are not, by themselves, evidence of a credential leak. **No leak is alleged or certified absent.** Before public repository visibility, review every reachable blob and retained ref, plus any release source archive; before public image distribution, audit the actual shipped source/dependency/native closure. Approve ownership and confidential-material findings and satisfy upstream notice/source obligations. Do not print credential candidates into workflow logs.

## Private preparation versus publication

The current [`release.yaml`](.github/workflows/release.yaml) defaults to `prepare-private`: it requires successful trusted main CI for the full source SHA and current immutable main, including `supplementary`, `chart`, and `publish`; retrieves that run's inspector image/chart producer evidence; verifies the existing image signature/issuer/exact-source SLSA provenance; retrieves SBOM/BuildKit provenance; and reuses the source-gated chart archive. The [`chart` gate](.github/workflows/ci.yaml) checks fresh inspector rendering without PVCs/legacy mounts, explicit retained-claim protection, and invalid/missing inputs—not broker/runner configurations. Requested stable or `beta.N` SemVer must match exact-source Chart.yaml and the packaged version. Preparation verifies archive SHA256 and uploads artifacts at existing repository visibility; a private repository's artifacts remain private. The manifest records `chartOciDigest: null` until an OCI push; archive SHA256 is not an OCI manifest digest. These are configured requirements, not claims that a current inspector release run passed.

Canonical upstream coordinates are `ghcr.io/samuelho-dev/coordinator` and `oci://ghcr.io/samuelho-dev/charts/coordinator`; forks derive their own repository image and owner chart namespace. Explicit `publish` publishes the chart and a GitHub release at existing visibility, signing/attesting the chart while retaining the trusted CI image producer's signature/provenance. It never rebuilds or re-signs a foreign image, changes source/package visibility, modifies branch policy, deploys infrastructure, or enrolls native hosts. GitHub release visibility follows repository visibility; the workflow does not make a repository public. Existing chart/release versions are refused rather than deliberately overwritten.

**Public-ready status remains unestablished.** Required outstanding evidence includes complete actual-shipped dependency/image notice/source compliance, native/standalone-binary compliance for any such redistributed artifacts (including historical releases), full-history/confidential-material/ownership review, explicit owner-approved visibility changes outside these workflows, anonymous image/chart access checks, and independent supported Linux amd64 clean-install/upgrade/legacy-retention acceptance. The retained state/databases/journals/backups/PVCs and historical migration source are not an inspector database requirement or proof of recovery. Nothing in this notice, passing local smoke, package license metadata, or a prepared release manifest certifies those steps.
