<h1>The CodeChat Editor</h1>
<h2>Design</h2>
<ol>
    <li><a href="README.md">CodeChat Editor Overview</a>
        <ol>
            <li><a href="README.md#how-to-run">How to run</a></li>
            <li><a href="README.md#vision">Vision</a></li>
            <li><a href="README.md#specification">Specification</a></li>
        </ol>
    </li>
    <li><a href="docs/implementation.md">Implementation</a></li>
</ol>
<h2>Implementation</h2>
<ol>
    <li>Server
        <ol>
            <li><a href="server/src/main.rs">main.rs</a></li>
            <li><a href="server/src/lib.rs">lib.rs</a></li>
            <li><a href="server/src/lexer.rs">lexer.rs</a>
                <ol>
                    <li><a
                            href="server/src/lexer/supported_languages.rs">supported_languages.rs</a>
                    </li>
                </ol>
            </li>
            <li><a href="server/src/webserver.rs">webserver.rs</a></li>
            <li><a href="server/Cargo.toml">Cargo.toml</a></li>
        </ol>
    </li>
    <li>Client
        <ol>
            <li>Editor
                <ol>
                    <li><a
                            href="client/src/CodeChatEditor.mts">CodeChatEditor.mts</a><br>
                        <ol>
                            <li><a
                                    href="client/src/ace-webpack.mts">ace-webpack.mts</a>
                            </li>
                            <li><a
                                    href="client/src/tinymce-webpack.mts">tinymce-webpack.mts</a>
                            </li>
                            <li><a
                                    href="client/src/EditorComponents.mts">EditorComponents.mts</a>
                            </li>
                            <li><a
                                    href="client/src/graphviz-webcomponent-setup.mts">graphviz-webcomponent-setup.mts</a>
                            </li>
                            <li><a
                                    href="client/src/typings.d.ts">typings.d.ts</a>
                            </li>
                        </ol>
                    </li>
                </ol>
            </li>
            <li>Styles
                <ol>
                    <li><a
                            href="client/static/css/CodeChatEditor.css">CodeChatEditor.css</a>
                    </li>
                    <li><a
                            href="client/static/css/CodeChatEditorProject.css">CodeChatEditorProject.css</a>
                    </li>
                    <li><a
                            href="client/static/css/CodeChatEditorSidebar.css">CodeChatEditorSidebar.css</a>
                    </li>
                </ol>
            </li>
            <li>Tests
                <ol>
                    <li><a
                            href="client/src/CodeChatEditor-test.mts">CodeChatEditor-test.mts</a>
                        <ol>
                            <li><a href="server/src/lib.rs?test">Run tests</a>
                            </li>
                        </ol>
                    </li>
                </ol>
            </li>
        </ol>
    </li>
    <li>Development tools
        <ol>
            <li>Git&nbsp;
                <ol>
                    <li>server/.gitignore</li>
                    <li>client/static/.gitignore</li>
                    <li>client/.gitignore</li>
                </ol>
            </li>
            <li>NPM/esbuild
                <ol>
                    <li>package.json</li>
                    <li><a href="client/tsconfig.json">tsconfig.json</a></li>
                    <li><a href="client/.eslintrc.yml">.eslintrc.yml</a></li>
                </ol>
            </li>
        </ol>
    </li>
</ol>
<h2>Misc</h2>
<ul>
    <li><a href="new-project-template/README.md" target="_blank"
            rel="noopener">New project template</a></li>
    <li><a href="toc.md">Table of contents</a></li>
    <li><a href="docs/index.md">Index</a></li>
</ul>
<h2>Notes</h2>
<ul>
    <li><a id="auto-title"></a>TODO: all links here should be auto-titled and
        autogenerate the page-local TOC.</li>
</ul>
<h2><a href="LICENSE.md">License</a></h2>