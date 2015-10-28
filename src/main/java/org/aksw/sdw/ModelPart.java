package org.aksw.sdw;

import org.openrdf.model.*;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.util.ModelException;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Chile on 3/21/2015.
 */
public class ModelPart implements Model {
    private Model model;
    private long streamStartLocation;
    private long streamEndLocation;

    public ModelPart(Model model, long start, long end)
    {
        this.model = new TreeModel(model);
        this.streamEndLocation = end;
        this.streamStartLocation = start;
    }

    public ModelPart(ModelPart model)
    {
        this.model = new TreeModel(model);
        this.streamStartLocation = model.getStreamStartLocation();
        this.streamEndLocation = model.getStreamEndLocation();
    }

    public ModelPart()
    {
        this.model = new TreeModel();
    }

    @Override
    public Model unmodifiable() {
        return model.unmodifiable();
    }

    @Override
    public Set<Namespace> getNamespaces() {
        return model.getNamespaces();
    }

    @Override
    public Namespace getNamespace(String s) {
        return model.getNamespace(s);
    }

    @Override
    public Namespace setNamespace(String s, String s1) {
        return model.setNamespace(s, s1);
    }

    @Override
    public void setNamespace(Namespace namespace) {
        model.setNamespace(namespace);
    }

    @Override
    public Namespace removeNamespace(String s) {
        return model.removeNamespace(s);
    }

    @Override
    public boolean contains(Resource resource, URI uri, Value value, Resource... resources) {
        return model.contains(resource, uri, value, resources);
    }

    @Override
    public ValueFactory getValueFactory() {
        return model.getValueFactory();
    }

    @Override
    public boolean add(Resource resource, URI uri, Value value, Resource... resources) {
        return model.add(resource, uri, value, resources);
    }

    @Override
    public Iterator<Statement> match(Resource resource, URI uri, Value value, Resource... resources) {
        return model.match(resource, uri, value, resources);
    }

    @Override
    public boolean clear(Resource... resources) {
        return model.clear(resources);
    }

    @Override
    public boolean remove(Resource resource, URI uri, Value value, Resource... resources) {
        return model.remove(resource, uri, value, resources);
    }

    @Override
    public Model filter(Resource resource, URI uri, Value value, Resource... resources) {
        return model.filter(resource, uri, value, resources);
    }

    @Override
    public Set<Resource> subjects() {
        return model.subjects();
    }

    @Override
    public Resource subjectResource() throws ModelException {
        return model.subjectResource();
    }

    @Override
    public URI subjectURI() throws ModelException {
        return model.subjectURI();
    }

    @Override
    public BNode subjectBNode() throws ModelException {
        return model.subjectBNode();
    }

    @Override
    public Set<URI> predicates() {
        return model.predicates();
    }

    @Override
    public Set<Value> objects() {
        return model.objects();
    }

    @Override
    public Set<Resource> contexts() {
        return model.contexts();
    }

    @Override
    public Value objectValue() throws ModelException {
        return model.objectValue();
    }

    @Override
    public Literal objectLiteral() throws ModelException {
        return model.objectLiteral();
    }

    @Override
    public Resource objectResource() throws ModelException {
        return model.objectResource();
    }

    @Override
    public URI objectURI() throws ModelException {
        return model.objectURI();
    }

    @Override
    public String objectString() throws ModelException {
        return model.objectString();
    }

    @Override
    public int size() {
        return model.size();
    }

    @Override
    public boolean isEmpty() {
        return model.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return model.contains(o);
    }

    @Override
    public Iterator<Statement> iterator() {
        return model.iterator();
    }

    @Override
    public Object[] toArray() {
        return model.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return model.toArray(a);
    }

    @Override
    public boolean add(Statement statement) {
        return model.add(statement);
    }

    @Override
    public boolean remove(Object o) {
        return model.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return model.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Statement> c) {
        return model.addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return model.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return model.retainAll(c);
    }

    @Override
    public void clear() {
        model.clear();
    }

    @Override
    public boolean equals(Object o) {
        return model.equals(o);
    }

    @Override
    public int hashCode() {
        return model.hashCode();
    }

    public long getStreamStartLocation() {
        return streamStartLocation;
    }

    public long getStreamEndLocation() {
        return streamEndLocation;
    }

    public void setStreamStartLocation(long streamStartLocation) {
        this.streamStartLocation = streamStartLocation;
    }

    public void setStreamEndLocation(long streamEndLocation) {
        this.streamEndLocation = streamEndLocation;
    }
}
